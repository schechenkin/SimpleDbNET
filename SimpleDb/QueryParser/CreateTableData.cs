using SimpleDb.Record;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDb.QueryParser
{
    public class CreateTableData
    {
        private String tblname;
        private Schema sch;

        /**
         * Saves the table name and schema.
         */
        public CreateTableData(String tblname, Schema sch)
        {
            this.tblname = tblname;
            this.sch = sch;
        }

        /**
         * Returns the name of the new table.
         * @return the name of the new table
         */
        public String tableName()
        {
            return tblname;
        }

        /**
         * Returns the schema of the new table.
         * @return the schema of the new table
         */
        public Schema newSchema()
        {
            return sch;
        }
    }
}
