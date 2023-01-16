namespace SimpleDB.Tx.Recovery
{
    public interface IRecoveryManager
    {
        void commit();
        void recover();
        void rollback();
        int setDateTime(Data.Buffer buff, int offset, DateTime dateTime);
        int setInt(Data.Buffer buff, int offset, int newval);
        int setString(Data.Buffer buff, int offset, string newval);
    }
}