using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GeoJsonObjectModel;
using System;
using System.Data;
using TAMU.GeoInnovation.Common.Utils.Databases.MongoDB;
using TAMU.GeoInnovation.PointIntersectors.Census.Census2010;
using USC.GISResearchLab.Common.Databases.QueryManagers;

namespace TAMU.GeoInnovation.PointIntersectors.Census.MongoDB.Census2010
{
    [Serializable]
    public class MongoDBCensus2010StatesPointIntersector : AbstractCensus2010StatesPointIntersector
    {

        #region Properties


        #endregion

        public MongoDBCensus2010StatesPointIntersector()
            : base()
        { }

        public MongoDBCensus2010StatesPointIntersector(double version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
            : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        { }



        public override DataTable GetRecordAsDataTable(double longitude, double latitude, string state, string county, double version)
        {
            DataTable ret = null;

            try
            {

                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    MongoDBConnection mongoDBConnection = (MongoDBConnection)CountryFilesQueryManager.Connection;
                    if (mongoDBConnection != null)
                    {
                        MongoServer mongoServer = mongoDBConnection.MongoServer;

                        if (mongoServer != null)
                        {

                            MongoDatabase database = mongoServer.GetDatabase(CountryFilesQueryManager.DefaultDatabase);
                            MongoCollection mongoCollection = database.GetCollection<BsonDocument>("us_state10");

                            GeoJson2DCoordinates geoJson2DCoordinates = new GeoJson2DCoordinates(longitude, latitude);
                            GeoJsonPoint<GeoJson2DCoordinates> point = GeoJson.Point(null, geoJson2DCoordinates);

                            MongoCursor<BsonDocument> docs = mongoCollection.FindAs<BsonDocument>(Query.GeoIntersects("shapeGeogAsGeoJSON", point));
                            ret = new DataTable();
                            if (docs != null && docs.Count() > 0)
                            {
                                ret.Columns.Add("stateFp10", typeof(string));
                                ret.Columns.Add("stUsPs10", typeof(string));
                            }
                            foreach (BsonDocument doc in docs)
                            {
                                ret.Rows.Add("stateFp10", doc["STATEFP10"].AsString);
                                ret.Rows.Add("stUsPs10", doc["STUSPS10"].AsString);
                            }
                        }
                    }
                    //string sql = "";
                    //sql += " SELECT ";
                    //sql += "  stateFp, ";
                    //sql += "  stUsPs ";
                    //sql += " FROM ";
                    //sql += " [Census2010CountryFiles].[dbo]." + "us_state ";
                    //sql += " WITH (INDEX (idx_geog))";
                    //sql += " WHERE ";
                    //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    //SqlCommand cmd = new SqlCommand(sql);
                    //IQueryManager qm = CountryFilesQueryManager;
                    //qm.AddParameters(cmd.Parameters);
                    //ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetStateFips: " + e.Message, e);
            }


            return ret;
        }

    }
}
