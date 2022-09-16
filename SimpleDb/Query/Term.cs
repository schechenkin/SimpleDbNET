using SimpleDB.Record;

namespace SimpleDB.Query
{
    public class Term
    {
        public enum CompareOperator { Equal, More, Less };
        
        private Expression lhs, rhs;
        private CompareOperator compareOperator;

        /**
         * Create a new term that compares two expressions
         * for equality.
         * @param lhs  the LHS expression
         * @param rhs  the RHS expression
         */
        public Term(Expression lhs, Expression rhs)
        {
            this.lhs = lhs;
            this.rhs = rhs;
            this.compareOperator = CompareOperator.Equal;
        }

        public Term(Expression lhs, Expression rhs, CompareOperator compareOperator)
        {
            this.lhs = lhs;
            this.rhs = rhs;
            this.compareOperator = compareOperator;
        }

        /**
         * Return true if both of the term's expressions
         * evaluate to the same constant,
         * with respect to the specified scan.
         * @param s the scan
         * @return true if both expressions have the same value in the scan
         */
        public bool isSatisfied(Scan s)
        {
            Constant lhsval = lhs.evaluate(s);
            Constant rhsval = rhs.evaluate(s);

            return compareOperator switch
            {
                CompareOperator.Equal => rhsval.Equals(lhsval),
                CompareOperator.More => lhsval.CompareTo(rhsval) > 0,
                CompareOperator.Less => lhsval.CompareTo(rhsval) < 0,
                _ => throw new ArgumentException("Invalid compareOperator", nameof(compareOperator)),
            };
        }

        /**
         * Calculate the extent to which selecting on the term reduces 
         * the number of records output by a query.
         * For example if the reduction factor is 2, then the
         * term cuts the size of the output in half.
         * @param p the query's plan
         * @return the integer reduction factor.
         */
        public int reductionFactor(Plan.Plan p)
        {
            String lhsName, rhsName;
            if (lhs.isFieldName() && rhs.isFieldName())
            {
                lhsName = lhs.asFieldName();
                rhsName = rhs.asFieldName();
                return Math.Max(p.distinctValues(lhsName),
                                p.distinctValues(rhsName));
            }
            if (lhs.isFieldName())
            {
                lhsName = lhs.asFieldName();
                return p.distinctValues(lhsName);
            }
            if (rhs.isFieldName())
            {
                rhsName = rhs.asFieldName();
                return p.distinctValues(rhsName);
            }
            // otherwise, the term equates constants
            if (lhs.asConstant().Equals(rhs.asConstant()))
                return 1;
            else
                return int.MaxValue;
        }

        /**
         * Determine if this term is of the form "F=c"
         * where F is the specified field and c is some constant.
         * If so, the method returns that constant.
         * If not, the method returns null.
         * @param fldname the name of the field
         * @return either the constant or null
         */
        public Constant equatesWithConstant(String fldname)
        {
            if (lhs.isFieldName() &&
                lhs.asFieldName().Equals(fldname) &&
                !rhs.isFieldName())
                return rhs.asConstant();
            else if (rhs.isFieldName() &&
                     rhs.asFieldName().Equals(fldname) &&
                     !lhs.isFieldName())
                return lhs.asConstant();
            else
                return null;
        }

        /**
         * Determine if this term is of the form "F1=F2"
         * where F1 is the specified field and F2 is another field.
         * If so, the method returns the name of that field.
         * If not, the method returns null.
         * @param fldname the name of the field
         * @return either the name of the other field, or null
         */
        public String equatesWithField(String fldname)
        {
            if (lhs.isFieldName() &&
                lhs.asFieldName().Equals(fldname) &&
                rhs.isFieldName())
                return rhs.asFieldName();
            else if (rhs.isFieldName() &&
                     rhs.asFieldName().Equals(fldname) &&
                     lhs.isFieldName())
                return lhs.asFieldName();
            else
                return null;
        }

        /**
         * Return true if both of the term's expressions
         * apply to the specified schema.
         * @param sch the schema
         * @return true if both expressions apply to the schema
         */
        public bool appliesTo(Schema sch)
        {
            return lhs.appliesTo(sch) && rhs.appliesTo(sch);
        }

        public override String ToString()
        {
            return lhs.ToString() + GetCompareOperatorStringPresentation(compareOperator) + rhs.ToString();
        }

        private static string GetCompareOperatorStringPresentation(CompareOperator compareOperator)
        {
            switch(compareOperator)
            {
                case CompareOperator.Equal: return "=";
                case CompareOperator.More: return ">";
                case CompareOperator.Less: return "<";
                default: throw new Exception("Unknownn CompareOperator");
            }
        }
    }
}
