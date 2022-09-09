using System;

namespace CovidDataLake.ContentIndexer.Extraction.Models
{
    public class StringWrapper : IComparable<StringWrapper>, IEquatable<StringWrapper>
    {
        public StringWrapper(string value)
        {
            Value = value;
        }
        public string Value { get; }

        public int CompareTo(StringWrapper other)
        {
            if (ReferenceEquals(this, other))
            {
                return 0;
            }

            return ReferenceEquals(null, other) ? 1 : string.Compare(Value, other.Value, StringComparison.Ordinal);
        }

        public bool Equals(StringWrapper other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return Value == other.Value;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj.GetType() == this.GetType() && Equals((StringWrapper)obj);
        }

        public override int GetHashCode()
        {
            return (Value != null ? Value.GetHashCode() : 0);
        }
    }
}
