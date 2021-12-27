package it.cavallium.dbengine.client;

import io.soabase.recordbuilder.core.RecordBuilder;
import java.nio.file.Path;

/**
 * A database volume is a directory in which the data of the database is stored.
 *
 * Volume path can be relative: if it's relative it will be relative to the default data directory
 *
 * Target size can be exceeded if all the volumes are full
 */
@RecordBuilder
public record DatabaseVolume(Path volumePath, long targetSizeBytes) {}
