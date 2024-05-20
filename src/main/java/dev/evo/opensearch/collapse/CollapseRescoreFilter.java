/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package dev.evo.opensearch.collapse;

import dev.evo.opensearch.collapse.rescore.CollapseRescorerBuilder;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScriptSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

public class CollapseRescoreFilter implements ActionFilter {
    public static final Setting<Integer> COLLAPSE_RESCORE_FILTER_ORDER = Setting.intSetting(
        "collapse.rescore.filter.order", 10, Setting.Property.NodeScope
    );

    private static final String SCRIPT_SORT_FIELD_NAME = "_collapse_script_sort";

    @SuppressWarnings("unchecked")
    private static final Comparator<Object> ANY_COMPARATOR = (first, second) -> {
        if (first == null) {
            if (second == null) {
                return 0;
            }
            return -1;
        }
        if (second == null) {
            return 1;
        }
        return ((Comparable<Object>) first).compareTo(second);
    };

    static final class TopGroup {
        final int collapsedIx;
        Object sortValue;
        float score;

        TopGroup(int collapsedIx, Object sortValue, float score) {
            this.collapsedIx = collapsedIx;
            this.sortValue = sortValue;
            this.score = score;
        }
    }

    private final int order;

    public CollapseRescoreFilter(final Settings settings) {
        order = COLLAPSE_RESCORE_FILTER_ORDER.get(settings);
    }

    @Override
    public int order() {
        return order;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (!SearchAction.INSTANCE.name().equals(action)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        final var searchRequest = (SearchRequest) request;
        var source = searchRequest.source();
        if (source == null) {
            source = SearchSourceBuilder.searchSource();
        }
        final var origSize = source.size();
        final var origFrom = source.from();

        final var searchExtensions = source.ext();
        final var searchExt = searchExtensions.stream()
            .filter(ext -> ext.getWriteableName().equals(CollapseSearchExtBuilder.NAME))
            .findFirst();
        if (searchExt.isEmpty()) {
            chain.proceed(task, action, request, listener);
            return;
        }

        final var collapseExt = (CollapseSearchExtBuilder) searchExt.get();

        source.from(0);
        // Set size equal to window size thus we will get right number of docs after a merge
        final var size = collapseExt.windowSize();
        source.size(size);

        final var groupField = collapseExt.groupField();
        source.docValueField(groupField);

        final var sorts = collapseExt.getSorts();
        String tmpSortField = null;
        int tmpReverseMul = 1;
        if (!sorts.isEmpty()) {
            final var sort = sorts.get(0);
            if (sort.order() == SortOrder.DESC) {
                tmpReverseMul = -1;
            }

            // We cannot return a group sort value within search docs due to next check:
            // https://github.com/opensearch-project/OpenSearch/blob/v6.8.13/
            // server/src/main/java/org/opensearch/search/query/QuerySearchResult.java#L130
            // So we will calculate it one more time as a script field
            if (sort instanceof FieldSortBuilder) {
                final var fieldSort = (FieldSortBuilder) sort;
                tmpSortField = fieldSort.getFieldName();
                source.docValueField(tmpSortField);
            } else if (sort instanceof ScriptSortBuilder) {
                tmpSortField = SCRIPT_SORT_FIELD_NAME;
                final var scriptSort = (ScriptSortBuilder) sort;
                source.scriptField(tmpSortField, scriptSort.script());
            }

        }
        final var sortField = tmpSortField;
        final var reverseMul = tmpReverseMul;

        source.addRescorer(
            new CollapseRescorerBuilder(collapseExt.groupField())
                .windowSize(collapseExt.windowSize())
                .shardSize(collapseExt.shardSize())
                .setSorts(collapseExt.getSorts())
        );

        var collapseListener = new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                final var resp = (SearchResponse) response;
                final var searchHits = resp.getHits();
                final var hits = searchHits.getHits();
                if (hits.length == 0) {
                    listener.onResponse(response);
                    return;
                }

                final var collapsedHits = new ArrayList<SearchHit>(hits.length);

                if (sortField == null) {
                    final var seenGroupValues = new HashSet<>();
                    for (var hit : hits) {
                        final var groupDocField = hit.field(groupField);
                        if (groupDocField != null) {
                            final var groupValue = groupDocField.getValue();
                            if (groupValue != null) {
                                if (seenGroupValues.add(groupValue)) {
                                    collapsedHits.add(hit);
                                }
                                continue;
                            }
                        }
                        collapsedHits.add(hit);
                    }
                } else {
                    final var topGroups = new HashMap<Object, TopGroup>();

                    // TODO: can we instantiate FieldComparator
                    //  based on the type of the first sort value?
                    //
                    // final var firstSortValue = Arrays.stream(hits)
                    //     .map(h -> h.field(sortField).getValue())
                    //     .filter(Objects::nonNull)
                    //     .findFirst();

                    for (var hit : hits) {
                        final var groupDocField = hit.field(groupField);
                        if (groupDocField == null) {
                            collapsedHits.add(hit);
                            continue;
                        }

                        final var groupValue = groupDocField.getValue();
                        if (groupValue == null) {
                            collapsedHits.add(hit);
                            continue;
                        }

                        final var topGroup = topGroups.get(groupValue);
                        if (topGroup == null) {
                            collapsedHits.add(hit);
                            topGroups.put(
                                groupValue,
                                new TopGroup(
                                    collapsedHits.size() - 1,
                                    hit.field(sortField).getValue(),
                                    hit.getScore()
                                )
                            );
                            continue;
                        }

                        final var sortGroupField = hit.field(sortField);
                        final var sortValue = sortGroupField.getValue();
                        if (
                            sortValue != null && topGroup.sortValue == null ||
                            reverseMul * ANY_COMPARATOR.compare(topGroup.sortValue, sortValue) > 0
                        ) {
                            hit.score(topGroup.score);
                            collapsedHits.set(topGroup.collapsedIx, hit);
                            topGroup.sortValue = sortValue;
                        }
                    }
                }

                var from = origFrom;
                if (from <= 0) {
                    from = 0;
                }
                var fromIndex = Math.min(from, collapsedHits.size());
                var size = origSize;
                if (size <= 0) {
                    size = 10;
                }
                var toIndex = Math.min(fromIndex + size, collapsedHits.size());
                final var pagedCollapsedHits = collapsedHits
                    .subList(fromIndex, toIndex)
                    .toArray(new SearchHit[0]);
                var totalHits = new TotalHits(collapsedHits.size(), TotalHits.Relation.EQUAL_TO);
                final var collapsedSearchHits = new SearchHits(
                    pagedCollapsedHits, totalHits, searchHits.getMaxScore()
                );

                final var internalResponse = new InternalSearchResponse(
                    collapsedSearchHits,
                    (InternalAggregations) resp.getAggregations(),
                    resp.getSuggest(),
                    new SearchProfileShardResults(resp.getProfileResults()),
                    resp.isTimedOut(),
                    resp.isTerminatedEarly(),
                    resp.getNumReducePhases()
                );
                @SuppressWarnings("unchecked") final var newResponse = (Response) new SearchResponse(
                    internalResponse,
                    resp.getScrollId(),
                    resp.getTotalShards(),
                    resp.getSuccessfulShards(),
                    resp.getSkippedShards(),
                    resp.getTook().millis(),
                    resp.getShardFailures(),
                    resp.getClusters()
                );

                listener.onResponse(newResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };

        chain.proceed(task, action, request, collapseListener);
    }
}
