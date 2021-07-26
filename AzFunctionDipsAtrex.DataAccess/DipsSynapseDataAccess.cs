using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace AzFunctionDipsAtrex.DataAccess
{
    public class DipsSynapseDataAccess
    {
        string ConnectionString = string.Empty;

        public DipsSynapseDataAccess(string conexion)
        {
            this.ConnectionString = conexion;
        }

        public List<string> GetRepeatedTrackingSynapse(string trackings)
        {
            SqlDataReader oReader = null;
            SqlCommand oCommand = null;
            SqlConnection oConn = null;

            List<string> response = new List<string>();
            oConn = new SqlConnection(ConnectionString);
            oConn.Open();

            oCommand = new SqlCommand("usp_la_dips_ValidRegisterVolcadoFtpAtrex", oConn);

            oCommand.CommandType = System.Data.CommandType.StoredProcedure;

            oCommand.Parameters.AddWithValue("@trackings", SqlDbType.NVarChar).Value = trackings;

            oReader = oCommand.ExecuteReader();

            if (oReader.HasRows)
            {
                response = GetDataToResponse(oReader);
            }

            CerrarConexionCommit(null, oConn);
            return response;
        }

        public bool InsertVolcadoDipsInSynapse(string trackings)
        {
            SqlDataReader oReader = null;
            SqlCommand oCommand = null;
            SqlConnection oConn = null;

            oConn = new SqlConnection(ConnectionString);
            oConn.Open();
            try
            {
                oCommand = new SqlCommand("usp_la_dips_insertVolcadoFtpAtrex", oConn);

                oCommand.CommandType = System.Data.CommandType.StoredProcedure;

                oCommand.Parameters.AddWithValue("@volcado_list", SqlDbType.NVarChar).Value = trackings;

                oReader = oCommand.ExecuteReader();

                CerrarConexionCommit(null, oConn);
                return true;
            }
            catch (Exception)
            {
                CerrarConexionCommit(null, oConn);
                return false;
            }
        }

        private List<string> GetDataToResponse(SqlDataReader oReader)
        {
            List<string> responseList = new List<string>();

            while (oReader.Read())
            {
                responseList.Add(string.IsNullOrEmpty(oReader["Guias_Asociadas"].ToString()) ? "" : oReader["Guias_Asociadas"].ToString());
            }
            return responseList;
        }

        private void CerrarConexionCommit(SqlTransaction oTrans, SqlConnection oConn)
        {
            if (!(oTrans == null))
            {
                oTrans.Commit();
            }
            if (!(oConn == null))
            {
                if (!(oConn.State == ConnectionState.Closed))
                {
                    oConn.Close();
                    oConn.Dispose();
                    oConn = null;
                }
            }
        }

    }
}
