using System.Diagnostics;
using SimpleDb.Query;
using SimpleDb.Types;
using SimpleDb.Record;

namespace SimpleDb.Query;

public class Expression
{
    readonly Constant constant;
    readonly string? fldname;
    readonly bool isConstant;

    public Expression(Constant val)
    {
        constant = val;
        isConstant = true;
        fldname = null;
    }

    public Expression(String fldname)
    {
        this.fldname = fldname;
        isConstant = false;
    }

    /**
     * Evaluate the expression with respect to the
     * current record of the specified scan.
     * @param s the scan
     * @return the value of the expression, as a Constant
     */
    public Constant evaluate(Scan s)
    {
        if(isConstant)
            return constant;
        else
        {
            Debug.Assert(fldname != null);
            return s.GetValue(fldname);
        }
        
        throw new Exception("unknown behaviour");
    }

    /**
     * Return true if the expression is a field reference.
     * @return true if the expression denotes a field
     */
    public bool isFieldName()
    {
        return fldname != null;
    }

    /**
     * Return the constant corresponding to a constant expression,
     * or null if the expression does not
     * denote a constant.
     * @return the expression as a constant
     */
    public Constant asConstant()
    {
        return constant;
    }

    /**
     * Return the field name corresponding to a constant expression,
     * or null if the expression does not
     * denote a field.
     * @return the expression as a field name
     */
    public String asFieldName()
    {
        Debug.Assert(fldname != null);
        return fldname;
    }

    /**
     * Determine if all of the fields mentioned in this expression
     * are contained in the specified schema.
     * @param sch the schema
     * @return true if all fields in the expression are in the schema
     */
    public bool appliesTo(Schema sch)
    {
        if(isConstant)
            return true;
        else
        {
            Debug.Assert(fldname != null);
            return sch.HasField(fldname);
        }
    }

    public override string ToString()
    {
        if(isConstant)
            return constant.ToString();
        else
        {
            Debug.Assert(fldname != null);
            return fldname;
        }
    }
}
