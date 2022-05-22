using SimpleDB.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.QueryParser
{
    public class ModifyData
    {
        private String tblname;
        private String fldname;
        private Expression newval;
        private Predicate predicate;

        /**
         * Saves the table name, the modified field and its new value, and the predicate.
         */
        public ModifyData(String tblname, String fldname, Expression newval, Predicate pred)
        {
            this.tblname = tblname;
            this.fldname = fldname;
            this.newval = newval;
            this.predicate = pred;
        }

        /**
         * Returns the name of the affected table.
         * @return the name of the affected table
         */
        public String tableName()
        {
            return tblname;
        }

        /**
         * Returns the field whose values will be modified
         * @return the name of the target field
         */
        public String targetField()
        {
            return fldname;
        }

        /**
         * Returns an expression.
         * Evaluating this expression for a record produces
         * the value that will be stored in the record's target field.
         * @return the target expression
         */
        public Expression newValue()
        {
            return newval;
        }

        /**
         * Returns the predicate that describes which
         * records should be modified.
         * @return the modification predicate
         */
        public Predicate pred()
        {
            return predicate;
        }
    }
}
