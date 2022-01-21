using System;

namespace CovidDataLake.Cloud.Amazon
{
    public class CloudException: Exception
    {
        public CloudException(Exception exception = null) : base("Cloud Exception", exception)
        {
            
        }
    }
}
