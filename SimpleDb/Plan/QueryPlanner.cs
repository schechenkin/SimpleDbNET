using SimpleDB.QueryParser;
using SimpleDB.Tx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Plan
{
    public interface QueryPlanner
    {

        /**
         * Creates a plan for the parsed query.
         * @param data the parsed representation of the query
         * @param tx the calling transaction
         * @return a plan for that query
         */
        public Plan createPlan(QueryData data, Transaction tx);
    }
}
