using SimpleDb.Plan;
using SimpleDb.Record;
using SimpleDb.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDb.Query
{
    public class Predicate
    {
        private List<Term> terms = new ();

        /**
         * Create an empty predicate, corresponding to "true".
         */
        public Predicate() { }

        /**
         * Create a predicate containing a single term.
         * @param t the term
         */
        public Predicate(Term t)
        {
            terms.Add(t);
        }

        /**
         * Modifies the predicate to be the conjunction of
         * itself and the specified predicate.
         * @param pred the other predicate
         */
        public void conjoinWith(Predicate pred)
        {
            terms.AddRange(pred.terms);
        }

        /**
         * Returns true if the predicate evaluates to true
         * with respect to the specified scan.
         * @param s the scan
         * @return true if the predicate is true in the scan
         */
        public bool isSatisfied(Scan s)
        {
            foreach (Term t in terms)
                if (!t.isSatisfied(s))
                    return false;
            return true;
        }

        /** 
         * Calculate the extent to which selecting on the predicate 
         * reduces the number of records output by a query.
         * For example if the reduction factor is 2, then the
         * predicate cuts the size of the output in half.
         * @param p the query's plan
         * @return the integer reduction factor.
         */
        public int reductionFactor(SimpleDb.Plan.Plan p)
        {
            int factor = 1;
            foreach (Term t in terms)
                factor *= t.reductionFactor(p);
            return factor;
        }

        /**
         * Return the subpredicate that applies to the specified schema.
         * @param sch the schema
         * @return the subpredicate applying to the schema
         */
        public Predicate? selectSubPred(Schema sch)
        {
            Predicate result = new Predicate();
            foreach (Term t in terms)
                if (t.appliesTo(sch))
                    result.terms.Add(t);
            if (result.terms.Count == 0)
                return null;
            else
                return result;
        }

        /**
         * Return the subpredicate consisting of terms that apply
         * to the union of the two specified schemas, 
         * but not to either schema separately.
         * @param sch1 the first schema
         * @param sch2 the second schema
         * @return the subpredicate whose terms apply to the union of the two schemas but not either schema separately.
         */
        public Predicate? joinSubPred(Schema sch1, Schema sch2)
        {
            Predicate result = new Predicate();
            Schema newsch = new Schema();
            newsch.AddAll(sch1);
            newsch.AddAll(sch2);
            foreach (Term t in terms)
                if (!t.appliesTo(sch1) &&
                      !t.appliesTo(sch2) &&
                      t.appliesTo(newsch))
                    result.terms.Add(t);
            if (result.terms.Count == 0)
                return null;
            else
                return result;
        }

        /**
         * Determine if there is a term of the form "F=c"
         * where F is the specified field and c is some constant.
         * If so, the method returns that constant.
         * If not, the method returns null.
         * @param fldname the name of the field
         * @return either the constant or null
         */
        public Constant? equatesWithConstant(String fldname)
        {
            foreach (Term t in terms)
            {
                Constant? c = t.equatesWithConstant(fldname);
                if (c != null)
                    return c;
            }
            return null;
        }

        /**
         * Determine if there is a term of the form "F1=F2"
         * where F1 is the specified field and F2 is another field.
         * If so, the method returns the name of that field.
         * If not, the method returns null.
         * @param fldname the name of the field
         * @return the name of the other field, or null
         */
        public String? equatesWithField(String fldname)
        {
            foreach (Term t in terms)
            {
                String? s = t.equatesWithField(fldname);
                if (s != null)
                    return s;
            }
            return null;
        }

        public override string ToString()
        {
            if (terms.Count == 0)
                return string.Empty;
            
            return string.Join(" and ", terms);
        }
    }
}
