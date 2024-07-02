# pre_upgrade_hooks.py
from alembic.operations import Operations, MigrateOperation
from alembic import op


def skip_drops():
    # Table drops
    existing_drop_table = Operations.drop_table

    def drop_table(operations, table_name, **kw):
        print(f"Skipping drop table '{table_name}'")
        return  # Skip the operation

    Operations.drop_table = drop_table

    # Column drops
    existing_drop_column = Operations.drop_column

    def drop_column(operations, table_name, column_name, **kw):
        print(f"Skipping drop column '{column_name}' from table '{table_name}'")
        return  # Skip the operation

    Operations.drop_column = drop_column

    # Index drops
    existing_drop_index = Operations.drop_index

    def drop_index(operations, index_name, table_name=None, **kw):
        print(f"Skipping drop index '{index_name}'")
        return  # Skip the operation

    Operations.drop_index = drop_index

    # Foreign key drops
    existing_drop_constraint = Operations.drop_constraint

    def drop_constraint(operations, constraint_name, table_name, type_=None, **kw):
        if type_ == 'foreignkey':
            print(f"Skipping drop foreign key '{constraint_name}'")
            return  # Skip the operation
        return existing_drop_constraint(operations, constraint_name, table_name, type_, **kw)

    Operations.drop_constraint = drop_constraint


def include_object(object, name, type_, reflected, compare_to):
    # Hook to include only specific objects
    return True


def run_migrations_online():
    from alembic import context
    skip_drops()
    connectable = context.config.attributes.get('connection', None)
    context.configure(
        connection=connectable,
        include_object=include_object,
        process_revision_directives=None,
        render_as_batch=None,
        target_metadata=None,
    )
    with context.begin_transaction():
        context.run_migrations()


if __name__ == "__main__":
    run_migrations_online()
