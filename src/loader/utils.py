import pyarrow as pa

def normalize_arrow_schema(arrow_table: pa.Table) -> pa.Table:
    new_fields = []

    for field in arrow_table.schema:
        field_type = field.type

        if pa.types.is_timestamp(field_type) and field_type.unit == "ns":
            new_fields.append(
                pa.field(
                    field.name,
                    pa.timestamp("us", tz=field_type.tz),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        elif pa.types.is_large_string(field_type):
            new_fields.append(
                pa.field(
                    field.name,
                    pa.string(),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        else:
            new_fields.append(field)

    new_schema = pa.schema(new_fields, metadata=arrow_table.schema.metadata)
    return arrow_table.cast(new_schema)