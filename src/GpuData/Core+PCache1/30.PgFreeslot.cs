namespace Core
{
    public class PgFreeslot
    {
        public PgFreeslot() { }
        public PgFreeslot(PgHdr p) { _PgHdr = p; }

        public PgFreeslot Next; // Next free slot
        internal PgHdr _PgHdr = new PgHdr();
    }
}
