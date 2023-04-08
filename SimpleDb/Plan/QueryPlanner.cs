using SimpleDb.Transactions;
using SimpleDb.QueryParser;

namespace SimpleDb.Plan;

public interface QueryPlanner
{

    /**
     * Creates a plan for the parsed query.
     * @param data the parsed representation of the query
     * @param tx the calling transaction
     * @return a plan for that query
     */
    public SimpleDb.Plan.Plan createPlan(QueryData data, Transaction tx);
}
