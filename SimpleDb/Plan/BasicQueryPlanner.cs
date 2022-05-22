using SimpleDB.Metadata;
using SimpleDB.QueryParser;
using SimpleDB.Tx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.Plan
{
    internal class BasicQueryPlanner : QueryPlanner
    {
        private MetadataMgr mdm;

        public BasicQueryPlanner(MetadataMgr mdm)
        {
            this.mdm = mdm;
        }

        /**
         * Creates a query plan as follows.  It first takes
         * the product of all tables and views; it then selects on the predicate;
         * and finally it projects on the field list. 
         */
        public Plan createPlan(QueryData data, Transaction tx)
        {
            //Step 1: Create a plan for each mentioned table or view.
            List<Plan> plans = new List<Plan>();
            foreach (String tblname in data.tables())
            {
                String viewdef = mdm.getViewDef(tblname, tx);
                if (viewdef != null)
                { // Recursively plan the view.
                    Parser parser = new Parser(viewdef);
                    QueryData viewdata = parser.query();
                    plans.Add(createPlan(viewdata, tx));
                }
                else
                    plans.Add(new TablePlan(tx, tblname, mdm));
            }

            //Step 2: Create the product of all table plans
            Plan p = plans[0];
            plans.RemoveAt(0);
            foreach (Plan nextplan in plans)
                p = new ProductPlan(p, nextplan);

            //Step 3: Add a selection plan for the predicate
            p = new SelectPlan(p, data.pred());

            //Step 4: Project on the field names
            p = new ProjectPlan(p, data.fields());
            return p;
        }
    }
}
