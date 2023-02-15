using SimpleDb.Indexes.Planner;
using SimpleDB.Indexes;
using SimpleDB.Metadata;
using SimpleDB.Query;
using SimpleDB.QueryParser;
using SimpleDB.QueryPlan;
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
        public Plan createPlan(QueryData queryData, Transaction tx)
        {
            //Step 1: Create a plan for each mentioned table or view.
            List<Plan> plans = new List<Plan>();
            foreach (String tblname in queryData.tables())
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
            //посмотреть на предикат и по возможности заменить на IndexSelectPlan
            if(p is TablePlan tablePlan)
            {
                var predicate = queryData.pred();
                var indexes = mdm.getIndexInfo(tablePlan.tableName, tx);
                bool indexFound = false;
                foreach(var columnName in indexes.Keys)
                {
                    Constant? val = predicate.equatesWithConstant(columnName);
                    if (val != null)
                    {
                        IndexInfo ii = indexes[columnName];
                        p = new IndexSelectPlan(p, ii, val.Value);
                        indexFound = true;
                        break;
                    }
                }

                if(!indexFound)
                {
                    p = new SelectPlan(tablePlan, predicate);
                }
            }
            else
            {
                p = new SelectPlan(p, queryData.pred());
            }

            //Step 4: Project on the field names
            p = new ProjectPlan(p, queryData.fields());
            return p;
        }
    }
}
