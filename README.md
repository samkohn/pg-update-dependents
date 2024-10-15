# pg-update-dependents

When you want to update an object in PostgreSQL
but it has dependents,
you need to drop all of the dependents, make your change,
and then re-create all of the dependents.
If there are complex dependencies, this process can get intense.
`pg-update-dependents` will automatically create a script
that drops all dependent views and then re-creates them,
all in the correct order,
so that you can make your change and be on your way.
