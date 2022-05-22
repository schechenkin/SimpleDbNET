using SimpleDB.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.QueryParser
{
    public class DeleteData
    {
        private String _tblname;
        private Predicate _pred;

        /**
         * Saves the table name and predicate.
         */
        public DeleteData(String tblname, Predicate pred)
        {
            _tblname = tblname;
            _pred = pred;
        }

        /**
         * Returns the name of the affected table.
         * @return the name of the affected table
         */
        public String tableName()
        {
            return _tblname;
        }

        /**
         * Returns the predicate that describes which
         * records should be deleted.
         * @return the deletion predicate
         */
        public Predicate pred()
        {
            return _pred;
        }
    }
}
