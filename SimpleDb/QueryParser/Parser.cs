using SimpleDB.Query;
using SimpleDB.Record;

namespace SimpleDB.QueryParser
{
    class Parser
    {
        private Lexer lex;

        public Parser(String s)
        {
            lex = new Lexer(s);
        }

        // Methods for parsing predicates, terms, expressions, constants, and fields

        public String field()
        {
            return lex.eatId();
        }

        public Constant constant()
        {
            if (lex.matchKeyword("null"))
            {
                lex.eatKeyword("null");
                return Constant.Null();
            }
            else if (lex.matchStringConstant())
                return new Constant(lex.eatStringConstant());
            else if (lex.matchIntConstant())
                return new Constant(lex.eatIntConstant());
            else
                return new Constant(lex.eatDateTimeConstant());
        }

        public Expression expression()
        {
            if (lex.matchId())
                return new Expression(field());
            else
                return new Expression(constant());
        }

        public Term term()
        {
            Expression lhs = expression();
            Expression rhs = null;
            Term.CompareOperator compareOperator = Term.CompareOperator.Equal;
            if (lex.matchDelim('='))
            {
                lex.eatDelim('=');
            }
            else if (lex.matchDelim('>'))
            {
                lex.eatDelim('>');
                compareOperator = Term.CompareOperator.More;
            }
            else if (lex.matchDelim('<'))
            {
                lex.eatDelim('<');
                compareOperator = Term.CompareOperator.Less;
            }
            else if(lex.matchKeyword("is"))
            {
                lex.eatKeyword("is");
                lex.eatKeyword("null");
                compareOperator = Term.CompareOperator.IsNull;
            }
            else
                throw new BadSyntaxException();

            if(compareOperator != Term.CompareOperator.IsNull && compareOperator != Term.CompareOperator.IsNotNull)
                rhs = expression();
            return new Term(lhs, rhs, compareOperator);
        }

        public Predicate predicate()
        {
            Predicate pred = new Predicate(term());
            if (lex.matchKeyword("and"))
            {
                lex.eatKeyword("and");
                pred.conjoinWith(predicate());
            }
            return pred;
        }

        // Methods for parsing queries

        public QueryData query()
        {
            lex.eatKeyword("select");
            List<String> fields = selectList();
            lex.eatKeyword("from");
            var tables = tableList();
            Predicate pred = new Predicate();
            if (lex.matchKeyword("where"))
            {
                lex.eatKeyword("where");
                pred = predicate();
            }
            return new QueryData(fields, tables, pred);
        }

        private List<String> selectList()
        {
            var L = new List<String>();
            L.Add(field());
            if (lex.matchDelim(','))
            {
                lex.eatDelim(',');
                L.AddRange(selectList());
            }
            return L;
        }

        private List<String> tableList()
        {
            var L = new List<String>();
            L.Add(lex.eatId());
            if (lex.matchDelim(','))
            {
                lex.eatDelim(',');
                L.AddRange(tableList());
            }
            return L;
        }

        // Methods for parsing the various update commands

        public Object updateCmd()
        {
            if (lex.matchKeyword("insert"))
                return insert();
            else if (lex.matchKeyword("delete"))
                return delete();
            else if (lex.matchKeyword("update"))
                return modify();
            else
                return create();
        }

        private Object create()
        {
            lex.eatKeyword("create");
            if (lex.matchKeyword("table"))
                return createTable();
            else if (lex.matchKeyword("view"))
                return createView();
            else
                return createIndex();
        }

        // Method for parsing delete commands

        public DeleteData delete()
        {
            lex.eatKeyword("delete");
            lex.eatKeyword("from");
            String tblname = lex.eatId();
            Predicate pred = new Predicate();
            if (lex.matchKeyword("where"))
            {
                lex.eatKeyword("where");
                pred = predicate();
            }
            return new DeleteData(tblname, pred);
        }

        // Methods for parsing insert commands

        public InsertData insert()
        {
            lex.eatKeyword("insert");
            lex.eatKeyword("into");
            String tblname = lex.eatId();
            lex.eatDelim('(');
            List<String> flds = fieldList();
            lex.eatDelim(')');
            lex.eatKeyword("values");
            lex.eatDelim('(');
            List<Constant> vals = constList();
            lex.eatDelim(')');
            return new InsertData(tblname, flds, vals);
        }

        private List<String> fieldList()
        {
            List<String> L = new ();
            L.Add(field());
            if (lex.matchDelim(','))
            {
                lex.eatDelim(',');
                L.AddRange(fieldList());
            }
            return L;
        }

        private List<Constant> constList()
        {
            List<Constant> L = new ();
            L.Add(constant());
            if (lex.matchDelim(','))
            {
                lex.eatDelim(',');
                L.AddRange(constList());
            }
            return L;
        }

        // Method for parsing modify commands

        public ModifyData modify()
        {
            lex.eatKeyword("update");
            String tblname = lex.eatId();
            lex.eatKeyword("set");
            String fldname = field();
            lex.eatDelim('=');
            Expression newval = expression();
            Predicate pred = new Predicate();
            if (lex.matchKeyword("where"))
            {
                lex.eatKeyword("where");
                pred = predicate();
            }
            return new ModifyData(tblname, fldname, newval, pred);
        }

        // Method for parsing create table commands

        public CreateTableData createTable()
        {
            lex.eatKeyword("table");
            String tblname = lex.eatId();
            lex.eatDelim('(');
            Schema sch = fieldDefs();
            lex.eatDelim(')');
            return new CreateTableData(tblname, sch);
        }

        private Schema fieldDefs()
        {
            Schema schema = fieldDef();
            if (lex.matchDelim(','))
            {
                lex.eatDelim(',');
                Schema schema2 = fieldDefs();
                schema.AddAll(schema2);
            }
            return schema;
        }

        private Schema fieldDef()
        {
            String fldname = field();
            return fieldType(fldname);
        }

        private Schema fieldType(String fldname)
        {
            Schema schema = new Schema();
            if (lex.matchKeyword("int"))
            {
                lex.eatKeyword("int");
                if (lex.matchKeyword("not"))
                {
                    lex.eatKeyword("not");
                    lex.eatKeyword("null");
                    schema.AddIntColumn(fldname, false);
                }
                else
                {
                    schema.AddIntColumn(fldname, true);
                }
            }
            else if (lex.matchKeyword("varchar"))
            {
                lex.eatKeyword("varchar");
                lex.eatDelim('(');
                int strLen = lex.eatIntConstant();
                lex.eatDelim(')');
                if (lex.matchKeyword("not"))
                {
                    lex.eatKeyword("not");
                    lex.eatKeyword("null");
                    schema.AddStringColumn(fldname, strLen, false);
                }
                else
                {
                    schema.AddStringColumn(fldname, strLen, true);
                }
            }
            else if (lex.matchKeyword("dateTime"))
            {
                lex.eatKeyword("dateTime");
                if (lex.matchKeyword("not"))
                {
                    lex.eatKeyword("not");
                    lex.eatKeyword("null");
                    schema.AddDateTimeColumn(fldname, false);
                }
                else
                {
                    schema.AddDateTimeColumn(fldname, false);
                }
            }
            else
                throw new BadSyntaxException();
            return schema;
        }

        // Method for parsing create view commands

        public CreateViewData createView()
        {
            lex.eatKeyword("view");
            String viewname = lex.eatId();
            lex.eatKeyword("as");
            QueryData qd = query();
            return new CreateViewData(viewname, qd);
        }


        //  Method for parsing create index commands

        public CreateIndexData createIndex()
        {
            lex.eatKeyword("index");
            String idxname = lex.eatId();
            lex.eatKeyword("on");
            String tblname = lex.eatId();
            lex.eatDelim('(');
            String fldname = field();
            lex.eatDelim(')');
            return new CreateIndexData(idxname, tblname, fldname);
        }
    }
}
