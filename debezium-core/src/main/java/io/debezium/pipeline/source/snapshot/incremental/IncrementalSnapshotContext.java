/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.relational.Table;

public interface IncrementalSnapshotContext<T> {

    DataCollection<T> currentDataCollectionId();

    DataCollection<T> nextDataCollection();

    List<DataCollection<T>> addDataCollectionNamesToSnapshot(String correlationId, List<String> dataCollectionIds, List<AdditionalCondition> additionalCondition,
                                                             String surrogateKey);

    int dataCollectionsToBeSnapshottedCount();

    boolean openWindow(String id);

    boolean closeWindow(String id);

    void pauseSnapshot();

    void resumeSnapshot();

    boolean isSnapshotPaused();

    boolean isNonInitialChunk();

    boolean snapshotRunning();

    void startNewChunk();

    void nextChunkPosition(Object[] lastKey);

    String currentChunkId();

    Object[] chunkEndPosititon();

    void sendEvent(Object[] keyFromRow);

    void maximumKey(Object[] key);

    Optional<Object[]> maximumKey();

    boolean deduplicationNeeded();

    Map<String, Object> store(Map<String, Object> offset);

    void revertChunk();

    void setSchema(Table schema);

    Table getSchema();

    boolean isSchemaVerificationPassed();

    void setSchemaVerificationPassed(boolean schemaVerificationPassed);

    void requestSnapshotStop(List<String> dataCollectionIds);

    boolean removeDataCollectionFromSnapshot(String dataCollectionId);

    List<DataCollection<T>> getDataCollections();

    void unsetCorrelationId();

    String getCorrelationId();

    List<String> getDataCollectionsToStop();

}
