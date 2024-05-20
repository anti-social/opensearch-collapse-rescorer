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

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.ScriptSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.TestCluster;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;

import org.junit.Ignore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertOrderedSearchHits;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertRequestBuilderThrows;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.hasScore;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;


@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class CollapseRescorerIT extends OpenSearchIntegTestCase {
    private static final String INDEX_NAME = "test_collapse";
    private static final String COLLAPSE_FIELD = "model_id";
    private static String PRICE_SCRIPT = "doc['price'].size() == 0 ? 0 : Math.log1p(doc['price'].value)";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            CollapseRescorePlugin.class
        );
    }

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        var testCluster = super.buildTestCluster(scope, seed);
        // Check we have at least 2 data nodes
        // so we test serializing/deserializing of search docs
        assertThat(testCluster.numDataNodes(), greaterThanOrEqualTo(2));
        return testCluster;
    }

    public void testEmptyIndex() throws IOException {
        createTestIndex(1);

        var queryBuilder = QueryBuilders.matchAllQuery();
        var response = client().prepareSearch(INDEX_NAME)
            .setSource(
                new SearchSourceBuilder()
                    .query(queryBuilder)
                    .ext(List.of(new CollapseSearchExtBuilder(COLLAPSE_FIELD)))
            )
            .get();

        assertSearchResponse(response);
    }

    public void testUnknownField() throws IOException {
        createAndPopulateTestIndex(1);

        var request = client().prepareSearch(INDEX_NAME)
            .setSource(
                new SearchSourceBuilder()
                    .query(rankQuery())
                    .ext(List.of(new CollapseSearchExtBuilder("missing_field")))
            );

        assertFailures(
            request,
            RestStatus.BAD_REQUEST,
            Matchers.containsString("no mapping found for `missing_field`")
        );
    }

    public void testEmptySearchSource() throws IOException {
        createAndPopulateTestIndex(1);

        var response = client().prepareSearch(INDEX_NAME)
            .get();

        assertSearchResponse(response);

        assertHitCount(response, 7);
    }

    public void testDefaultCollapsing() throws IOException {
        createAndPopulateTestIndex(1);

        var response = client().prepareSearch(INDEX_NAME)
            .setSource(
                new SearchSourceBuilder()
                    .query(rankQuery())
                    .ext(List.of(new CollapseSearchExtBuilder(COLLAPSE_FIELD)))
            )
            .get();

        assertSearchResponse(response);

        assertHitCount(response, 4);
        assertOrderedSearchHits(response, "5", "4", "3", "2");

        assertSearchHit(
            response, 1,
            hasFields(
                new DocumentField("model_id", List.of())
            )
        );
        assertSearchHit(
            response, 2,
            hasFields(
                new DocumentField("model_id", List.of(1L))
            )
        );
        assertSearchHit(
            response, 3,
            hasFields(
                new DocumentField("model_id", List.of())
            )
        );
        assertSearchHit(
            response, 4,
            hasFields(
                new DocumentField("model_id", List.of(2L))
            )
        );
    }

    public void testCollapsingSize() throws IOException {
        createAndPopulateTestIndex(1);

        var response = client().prepareSearch(INDEX_NAME)
            .setSource(
                new SearchSourceBuilder()
                    .query(rankQuery())
                    .ext(List.of(new CollapseSearchExtBuilder(COLLAPSE_FIELD)))
                    .size(2)
            )
            .get();

        assertSearchResponse(response);

        assertHitCount(response, 4);
        assertOrderedSearchHits(response, "5", "4");
    }

    public void testMerge() throws IOException {
        createAndPopulateTestIndex(2);

        // Ensure docs fell into different shards
        var response = client().prepareSearch(INDEX_NAME)
            .setQuery(QueryBuilders.idsQuery().addIds("4"))
            .setPreference("_shards:1")
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 1);

        response = client().prepareSearch(INDEX_NAME)
            .setQuery(QueryBuilders.idsQuery().addIds("7"))
            .setPreference("_shards:0")
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 1);

        response = client().prepareSearch(INDEX_NAME)
            .setSource(
                new SearchSourceBuilder()
                    .query(rankQuery())
                    .ext(List.of(new CollapseSearchExtBuilder(COLLAPSE_FIELD)))
                    .size(2)
            )
            .get();

        assertSearchResponse(response);

        assertHitCount(response, 4);
        assertOrderedSearchHits(response, "5", "4");
    }

    public void testFieldSort() throws IOException {
        createAndPopulateTestIndex(1);
        checkFieldSort();
    }

    public void testFieldSortReverse() throws IOException {
        createAndPopulateTestIndex(1);
        checkFieldSortReverse();
    }

    public void testFieldSortMerge() throws IOException {
        createAndPopulateTestIndex(2);
        checkFieldSort();
    }

    public void testFieldSortReverseMerge() throws IOException {
        createAndPopulateTestIndex(2);
        checkFieldSortReverse();
    }

    @Ignore
    public void testScriptSort() throws IOException {
        createAndPopulateTestIndex(1);
        checkScriptSort();
    }

    @Ignore
    public void testScriptSortMerge() throws IOException {
        createAndPopulateTestIndex(2);
        checkScriptSort();
    }

    public void testMultipleSort() throws IOException {
        createAndPopulateTestIndex(1);

        assertRequestBuilderThrows(
            client().prepareSearch(INDEX_NAME)
                .setSource(
                    new SearchSourceBuilder()
                        .query(rankQuery())
                        .ext(List.of(
                            new CollapseSearchExtBuilder(COLLAPSE_FIELD)
                                .addSort(SortBuilders.fieldSort("price"))
                                .addSort(SortBuilders.fieldSort("rank"))
                        ))
                ),
            RestStatus.BAD_REQUEST
        );
    }

    private void checkFieldSort() {
        var response = client().prepareSearch(INDEX_NAME)
            .setSource(
                new SearchSourceBuilder()
                    .query(rankQuery())
                    .ext(List.of(
                        new CollapseSearchExtBuilder(COLLAPSE_FIELD)
                            .addSort(SortBuilders.fieldSort("price"))
                    ))
            )
            .get();

        assertSearchResponse(response);

        assertHitCount(response, 4);
        assertOrderedSearchHits(response, "5", "1", "3", "6");

        // Scores must be replaced from the best hit in a group
        assertSearchHit(response, 1, hasScore(1.5F));
        assertSearchHit(response, 2, hasScore(1.4F));
        assertSearchHit(response, 3, hasScore(1.3F));
        assertSearchHit(response, 4, hasScore(1.2F));

        assertSearchHit(
            response, 1,
            hasFields(
                new DocumentField("model_id", List.of()),
                new DocumentField("price", List.of())
            )
        );
        assertSearchHit(
            response, 2,
            hasFields(
                new DocumentField("model_id", List.of(1L)),
                new DocumentField("price", List.of(Float.valueOf(0.01F).doubleValue()))
            )
        );
        assertSearchHit(
            response, 3,
            hasFields(
                new DocumentField("model_id", List.of()),
                new DocumentField("price", List.of())
            )
        );
        assertSearchHit(
            response, 4,
            hasFields(
                new DocumentField("model_id", List.of(2L)),
                new DocumentField("price", List.of(11.0))
            )
        );
    }

    private void checkFieldSortReverse() {
        var response = client().prepareSearch(INDEX_NAME)
            .setSource(
                new SearchSourceBuilder()
                    .query(rankQuery())
                    .ext(List.of(
                        new CollapseSearchExtBuilder(COLLAPSE_FIELD)
                            .addSort(SortBuilders.fieldSort("price").order(SortOrder.DESC))
                    ))
            )
            .get();

        assertSearchResponse(response);

        assertHitCount(response, 4);
        assertOrderedSearchHits(response, "5", "7", "3", "2");

        // Scores must be replaced from the best hit in a group
        assertSearchHit(response, 1, hasScore(1.5F));
        assertSearchHit(response, 2, hasScore(1.4F));
        assertSearchHit(response, 3, hasScore(1.3F));
        assertSearchHit(response, 4, hasScore(1.2F));

        assertSearchHit(
            response, 1,
            hasFields(
                new DocumentField("model_id", List.of()),
                new DocumentField("price", List.of())
            )
        );
        assertSearchHit(
            response, 2,
            hasFields(
                new DocumentField("model_id", List.of(1L)),
                new DocumentField("price", List.of(Float.valueOf(9.99F).doubleValue()))
            )
        );
        assertSearchHit(
            response, 3,
            hasFields(
                new DocumentField("model_id", List.of()),
                new DocumentField("price", List.of())
            )
        );
        assertSearchHit(
            response, 4,
            hasFields(
                new DocumentField("model_id", List.of(2L)),
                new DocumentField("price", List.of(12.0))
            )
        );
    }

    private void checkScriptSort() {
        var response = client().prepareSearch(INDEX_NAME)
            .setSource(
                new SearchSourceBuilder()
                    .query(rankQuery())
                    .ext(List.of(
                        new CollapseSearchExtBuilder(COLLAPSE_FIELD)
                            .addSort(SortBuilders.scriptSort(
                                new Script(
                                    ScriptType.INLINE,
                                    "painless",
                                    PRICE_SCRIPT,
                                    Collections.emptyMap()
                                ),
                                ScriptSortBuilder.ScriptSortType.NUMBER
                            ))
                    ))
            )
            .get();

        assertSearchResponse(response);

        assertHitCount(response, 4);
        assertOrderedSearchHits(response, "5", "4", "3", "6");

        // Scores must be replaced from the best hit in a group
        assertSearchHit(response, 1, hasScore(1.5F));
        assertSearchHit(response, 2, hasScore(1.4F));
        assertSearchHit(response, 3, hasScore(1.3F));
        assertSearchHit(response, 4, hasScore(1.2F));

        assertSearchHit(
            response, 1,
            hasFields(
                new DocumentField("model_id", List.of()),
                new DocumentField("_collapse_script_sort", List.of(0.0))
            )
        );
        assertSearchHit(
            response, 2,
            hasFields(
                new DocumentField("model_id", List.of(1L)),
                new DocumentField("_collapse_script_sort", List.of(0.0))
            )
        );
        assertSearchHit(
            response, 3,
            hasFields(
                new DocumentField("model_id", List.of()),
                new DocumentField("_collapse_script_sort", List.of(0.0))
            )
        );
        assertSearchHit(
            response, 4,
            hasFields(
                new DocumentField("model_id", List.of(2L)),
                new DocumentField("_collapse_script_sort", List.of(2.4849066497880004))
            )
        );
    }

    private void createTestIndex(int numberOfShards) throws IOException {
        assertAcked(
            prepareCreate(INDEX_NAME)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                )
                .setMapping(testMapping())
        );
        ensureGreen(INDEX_NAME);
    }

    private void createAndPopulateTestIndex(int numberOfShards) throws IOException {
        createTestIndex(numberOfShards);
        for (var docs : testDocs()) {
            final var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (var doc : docs) {
                bulk.add(doc.setIndex(INDEX_NAME));
            }
            bulk.get();
        }
        refresh(INDEX_NAME);
    }

    private QueryBuilder rankQuery() {
        return QueryBuilders.functionScoreQuery(
            new FieldValueFactorFunctionBuilder("rank")
        );
    }

    private XContentBuilder testMapping() throws IOException {
        return jsonBuilder()
            .startObject()
                // .startObject("_doc")
                    .startObject("properties")
                        .startObject(COLLAPSE_FIELD)
                            .field("type", "integer")
                        .endObject()
                        .startObject("rank")
                            .field("type", "float")
                        .endObject()
                        .startObject("price")
                            .field("type", "float")
                        .endObject()
                    .endObject()
                // .endObject()
            .endObject();
    }

    private List<List<IndexRequestBuilder>> testDocs() {
        return List.of(
            List.of(
                client().prepareIndex()
                    .setId("1")
                    .setSource(
                        COLLAPSE_FIELD, 1,
                        "rank", 1.1F,
                        "price", 0.01F
                    ),
                client().prepareIndex()
                    .setId("2")
                    .setSource(
                        COLLAPSE_FIELD, 2,
                        "rank", 1.2F,
                        "price", 12F
                    ),
                client().prepareIndex()
                    .setId("3")
                    .setSource(
                        "rank", 1.3F
                    ),
                client().prepareIndex()
                    .setId("4")
                    .setSource(
                        COLLAPSE_FIELD, 1,
                        "rank", 1.4F
                    )
            ),
            List.of(
                client().prepareIndex()
                    .setId("5")
                    .setSource(
                        "rank", 1.5F
                    ),
                client().prepareIndex()
                    .setId("6")
                    .setSource(
                        COLLAPSE_FIELD, 2,
                        "rank", 0.6F,
                        "price", 11F
                    ),
                client().prepareIndex()
                    .setId("7")
                    .setSource(
                        COLLAPSE_FIELD, 1,
                        "rank", 1.39F,
                        "price", 9.99F
                    )
            )
        );
    }

    private FieldsMatcher hasFields(DocumentField... fields) {
        return new FieldsMatcher(Arrays.asList(fields));
    }

    static class FieldsMatcher extends BaseMatcher<SearchHit> {
        private final Map<String, DocumentField> fields;

        FieldsMatcher(List<DocumentField> fields) {
            this.fields = fields.stream()
                .collect(Collectors.toMap(DocumentField::getName, f -> f));
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue(fields);
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            if (item instanceof SearchHit) {
                final var hit = (SearchHit) item;
                item = hit.getFields();
            }
            super.describeMismatch(item, description);
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof SearchHit)) {
                return false;
            }
            final var hit = (SearchHit) item;
            final var hitFields = hit.getFields();
            return fields.equals(hitFields);
        }
    }
}
