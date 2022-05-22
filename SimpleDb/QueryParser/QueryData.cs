using SimpleDB.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDB.QueryParser
{
    public class QueryData
    {
        private List<String> _fields;
        private List<String> _tables;
        private Predicate _pred;

        /**
         * Saves the field and table list and predicate.
         */
        public QueryData(List<String> fields, List<String> tables, Predicate pred)
        {
            this._fields = fields;
            this._tables = tables;
            this._pred = pred;
        }

        /**
         * Returns the fields mentioned in the select clause.
         * @return a list of field names
         */
        public List<String> fields()
        {
            return _fields;
        }

        /**
         * Returns the tables mentioned in the from clause.
         * @return a collection of table names
         */
        public List<String> tables()
        {
            return _tables;
        }

        /**
         * Returns the predicate that describes which
         * records should be in the output table.
         * @return the query predicate
         */
        public Predicate pred()
        {
            return _pred;
        }

        public override String ToString()
        {
            String result = "select ";
            foreach (String fldname in _fields)
                result += fldname + ", ";
            result = result.Substring(0, result.Length - 2); //remove final comma
            result += " from ";
            foreach (String tblname in _tables)
                result += tblname + ", ";
            result = result.Substring(0, result.Length - 2); //remove final comma
            String predstring = _pred.ToString();
            if (!predstring.Equals(""))
                result += " where " + predstring;
            return result;
        }
    }
}
