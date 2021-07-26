using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Globalization;
using AzFunctionDipsAtrex.DataAccess;
using System.Linq;
using AzFunctionDipsAtrex.Models;

namespace AzFunctionDipsAtrex
{
    public class AzFunctionParseVolcadoDips
    {
        private DipsSynapseDataAccess _dipsSynapseDataAccess;
        private DipsSorterDataAccess _dipsSorterDataAccess;
        private string _synapseConnectionString;
        private string _sorterConnectionString;
        private int _insertTotal;

        public AzFunctionParseVolcadoDips()
        {
            ReadEnviromentVariables();
            _dipsSynapseDataAccess = new DipsSynapseDataAccess(_synapseConnectionString);
            _dipsSorterDataAccess = new DipsSorterDataAccess(_sorterConnectionString);
        }

        [FunctionName("AzFunctionParseVolcadoDips")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] string req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            try
            {
                List<VolcadoDipsResponse> volcadoList = MapVolcadoDips(req, log);
                log.LogInformation("C# Total ots volcado: " + volcadoList.Count());

                string trackingList = GetListNewTrackingsDips(volcadoList);

                //// cargar datos en synapse
                //var validationResponse = _dipsSynapseDataAccess.GetRepeatedTrackingSynapse(trackingList);
                //log.LogInformation("C# Total ots synapse repetidas: " + validationResponse.Count());
                //var cleanRepet = volcadoList.Where(x => !validationResponse.Contains(x.GuiasAsociadas)).ToList();//!x.Master.Equals(validationResponse.Select(x => x))).Select(x => x).ToList();
                //int response = InsertDataInSynapse(cleanRepet);
                //log.LogInformation("C# Total synapse insertadas: " + response);

                // cargar datos en synapse
                var validationSorterResponse = _dipsSorterDataAccess.GetRepeatedTrackingSynapse(trackingList);
                log.LogInformation("C# Total ots sorter repetidas: " + validationSorterResponse.Count());
                var cleanSorterRepet = volcadoList.Where(x => !validationSorterResponse.Contains(x.GuiasAsociadas)).ToList();//!x.Master.Equals(validationResponse.Select(x => x))).Select(x => x).ToList();
                int responseSorter = InsertDataInSorterDB(cleanSorterRepet);
                log.LogInformation("C# Total sorter insertadas: " + responseSorter);

                log.LogInformation("C# HTTP trigger function end");
                return new OkObjectResult(response);
            }
            catch (Exception ex)
            {
                log.LogCritical("C# exception: " + ex.Message);
                return new OkObjectResult(ex.Message);
            }

        }

        private int InsertDataInSynapse(List<VolcadoDipsResponse> cleanRepet)
        {
            List<VolcadoDipsResponse> volcadoDips = new List<VolcadoDipsResponse>();

            var total = 0;
            var count = 0;

            foreach (var item in cleanRepet)
            {
                count += 1;
                total += 1;
                volcadoDips.Add(item);

                if (count == _insertTotal || total == cleanRepet.Count())
                {
                    string insert = JsonConvert.SerializeObject(volcadoDips);

                    if (_dipsSynapseDataAccess.InsertVolcadoDipsInSynapse(insert))
                    {
                        volcadoDips.Clear();
                        count = 0;
                    }
                }
            }
            return total;
        }

        private int InsertDataInSorterDB(List<VolcadoDipsResponse> cleanRepet)
        {
            List<VolcadoDipsResponse> volcadoDips = new List<VolcadoDipsResponse>();

            var total = 0;
            var count = 0;

            foreach (var item in cleanRepet)
            {
                count += 1;
                total += 1;
                volcadoDips.Add(item);

                if (count == _insertTotal || total == cleanRepet.Count())
                {
                    string insert = JsonConvert.SerializeObject(volcadoDips);

                    if (_dipsSorterDataAccess.InsertVolcadoDipsInSorter(insert))
                    {
                        volcadoDips.Clear();
                        count = 0;
                    }
                }
            }
            return total;
        }

        private string GetListNewTrackingsDips(List<VolcadoDipsResponse> response)
        {
            return string.Join(";", response.Select(x => x.Master).ToList());
        }

        private List<VolcadoDipsResponse> MapVolcadoDips(string req, ILogger log)
        {
            List<VolcadoDipsResponse> listResponse = new List<VolcadoDipsResponse>();
            var response = Regex.Replace(req.Replace(System.Environment.NewLine, string.Empty), @"\\r\\n", string.Empty);
            var responses = response.Split(';');

            int count = 0;

            for (int i = 0; i < responses.Length - 1; i++)
            {
                var unit = responses[i].Split('|');

                var single = new VolcadoDipsResponse();

                single.Empresa = unit[0];
                single.NumeroDIPSCompleta = unit[1];
                single.NumeroDIPS = unit[2];
                single.FechaVigencia = DateTime.ParseExact(FormatDatetime(unit[3]), "dd-MM-yyyy", CultureInfo.InvariantCulture);
                single.Codaduana = unit[4];
                single.CodOperacion = unit[5];
                single.FechaAceptacion = DateTime.ParseExact(FormatDatetime(unit[6]), "dd-MM-yyyy", CultureInfo.InvariantCulture);
                single.NombreEmpresa = unit[7];
                single.DireccionImportador = unit[8];
                single.CodComunaImportador = unit[9];
                single.Importador = unit[10];
                single.Comuna = unit[11];
                single.CodImportador = unit[12];
                single.RutImportador = unit[13];
                single.Digito = unit[14];
                single.PaisOrigen = unit[15];
                single.CodPaisOrigen = unit[16];
                single.PaisAdquisicion = unit[17];
                single.CodPaisAdquisicion = unit[18];
                single.CodviaTransporte = unit[19];
                single.PuertoEmbarque = unit[20];
                single.CodPuertoEmbarque = unit[21];
                single.PuertoDesembarque = unit[22];
                single.CodPuertoDesembarque = unit[23];
                single.Almacenista = unit[24];
                single.CodAlmacenista = unit[25];
                single.FechaRecepcion = DateTime.ParseExact(FormatDatetime(unit[26]), "dd-MM-yyyy", CultureInfo.InvariantCulture);
                single.FechaRetiro = DateTime.ParseExact(FormatDatetime(unit[27]), "dd-MM-yyyy", CultureInfo.InvariantCulture);
                single.NumeroManifiesto = unit[28];
                single.FechaManifiesto = DateTime.ParseExact(FormatDatetime(unit[29]), "dd-MM-yyyy", CultureInfo.InvariantCulture);
                single.FechaDocTransporte = DateTime.ParseExact(FormatDatetime(unit[30]), "dd-MM-yyyy", CultureInfo.InvariantCulture);
                single.Master = unit[31];
                single.Regimen = unit[32];
                single.CodRegimen = unit[33];
                single.TotalItem = unit[34];
                single.ValorFOB = Convert.ToDecimal(unit[35]);
                single.TotalHojas = unit[36];
                single.Flete = Convert.ToDecimal(unit[37]);
                single.TotalBultos = unit[38];
                single.Seguro = Convert.ToDecimal(unit[39]);
                single.Peso = Convert.ToDecimal(unit[40]);
                single.TotalCIF = Convert.ToDecimal(unit[41]);
                single.DescripcionArancel = unit[42];
                single.Ajuste = unit[43];
                single.GuiasCant = unit[44];
                single.PrecioFOB = Convert.ToDecimal(unit[45]);
                single.CodArancel = unit[46];
                single.ValorCIF = Convert.ToDecimal(unit[47]);
                single.AdValorem = Convert.ToDecimal(unit[48]);
                single.CodAdvalorem = unit[49];
                single.CIF1 = Convert.ToDecimal(unit[50]);
                single.IVA = Convert.ToDecimal(unit[51]);
                single.CodIVA = unit[52];
                single.IVASinSeguro = Convert.ToDecimal(unit[53]);
                single.TipoBulto = unit[54];
                single.CodBultos = unit[55];
                single.CantBulto = unit[56];
                single.Valor178 = Convert.ToDecimal(unit[57]);
                single.Valor191 = Convert.ToDecimal(unit[58]);
                single.CodImpto = unit[59];
                single.Impuesto = Convert.ToDecimal(unit[60]);
                single.CodAlmacen = unit[61];
                single.Valoralmacenaje = Convert.ToDecimal(unit[62]);
                single.CodCuenta = unit[63];
                single.Impto = unit[64];
                single.CodImptoAdicional = unit[65];
                single.PorcentajeImpuesto = unit[66];
                single.ValorImpuesto = Convert.ToDecimal(unit[67]);
                single.Despachador = unit[68];
                single.Aduana = unit[69];
                single.TipoOperacion = unit[70];
                single.Inspeccion = unit[71];
                single.CodInspecc = unit[72];
                single.DatoInterno = unit[73];
                single.UnidadMedida = unit[74];
                single.Dolar = Convert.ToDecimal(unit[75]);
                single.Valor91 = Convert.ToDecimal(unit[76]);
                single.Comentario1 = unit[77];
                single.Comentario2 = unit[78];
                single.Comentario3 = unit[79];
                single.Atributo = unit[80];
                single.Atributo2 = unit[81];
                single.Atributo3 = unit[82];
                single.Atributo4 = unit[83];
                single.Atributo5 = unit[84];
                single.Atributo6 = unit[85];
                single.DatoInternoAtrex = unit[86];
                single.DatointernoVacio = unit[87];
                single.FechaConfeccion = string.IsNullOrEmpty(unit[88]) ? DateTime.MinValue : DateTime.ParseExact(unit[88], "dd-MM-yyyy H:mm:ss", null);
                single.ArancelTratado = unit[89];
                single.CodigoAcuerdo = unit[90];
                single.GuiasAsociadas = unit[91];

                listResponse.Add(single);

                count += 1;

                //log.LogInformation("C# Count: " + count);
            }

            //foreach (var item in responses)
            //{
                
            //    //listResponse.Add(new VolcadoDipsResponse()
            //    //{
            //    //    Empresa = unit[0],
            //    //    NumeroDIPSCompleta = unit[1],
            //    //    NumeroDIPS = unit[2],
            //    //    FechaVigencia = DateTime.ParseExact(FormatDatetime(unit?[3]), "dd-MM-yyyy", CultureInfo.InvariantCulture),
            //    //    Codaduana = unit[4],
            //    //    CodOperacion = unit[5],
            //    //    FechaAceptacion = DateTime.ParseExact(FormatDatetime(unit[6]), "dd-MM-yyyy", CultureInfo.InvariantCulture),
            //    //    NombreEmpresa = unit[7],
            //    //    DireccionImportador = unit[8],
            //    //    CodComunaImportador = unit[9],
            //    //    Importador = unit[10],
            //    //    Comuna = unit[11],
            //    //    CodImportador = unit[12],
            //    //    RutImportador = unit[13],
            //    //    Digito = unit[14],
            //    //    PaisOrigen = unit[15],
            //    //    CodPaisOrigen = unit[16],
            //    //    PaisAdquisicion = unit[17],
            //    //    CodPaisAdquisicion = unit[18],
            //    //    CodviaTransporte = unit[19],
            //    //    PuertoEmbarque = unit[20],
            //    //    CodPuertoEmbarque = unit[21],
            //    //    PuertoDesembarque = unit[22],
            //    //    CodPuertoDesembarque = unit[23],
            //    //    Almacenista = unit[24],
            //    //    CodAlmacenista = unit[25],
            //    //    FechaRecepcion = DateTime.ParseExact(FormatDatetime(unit[26]), "dd-MM-yyyy", CultureInfo.InvariantCulture),
            //    //    FechaRetiro = DateTime.ParseExact(FormatDatetime(unit[27]), "dd-MM-yyyy", CultureInfo.InvariantCulture),
            //    //    NumeroManifiesto = unit[28],
            //    //    FechaManifiesto = DateTime.ParseExact(FormatDatetime(unit[29]), "dd-MM-yyyy", CultureInfo.InvariantCulture),
            //    //    FechaDocTransporte = DateTime.ParseExact(FormatDatetime(unit[30]), "dd-MM-yyyy", CultureInfo.InvariantCulture),
            //    //    Master = unit[31],
            //    //    Regimen = unit[32],
            //    //    CodRegimen = unit[33],
            //    //    TotalItem = unit[34],
            //    //    ValorFOB = Convert.ToDecimal(unit[35]),
            //    //    TotalHojas = unit[36],
            //    //    Flete = Convert.ToDecimal(unit[37]),
            //    //    TotalBultos = unit[38],
            //    //    Seguro = Convert.ToDecimal(unit[39]),
            //    //    Peso = Convert.ToDecimal(unit[40]),
            //    //    TotalCIF = Convert.ToDecimal(unit[41]),
            //    //    DescripcionArancel = unit[42],
            //    //    Ajuste = unit[43],
            //    //    GuiasCant = unit[44],
            //    //    PrecioFOB = Convert.ToDecimal(unit[45]),
            //    //    CodArancel = unit[46],
            //    //    ValorCIF = Convert.ToDecimal(unit[47]),
            //    //    AdValorem = Convert.ToDecimal(unit[48]),
            //    //    CodAdvalorem = unit[49],
            //    //    CIF1 = Convert.ToDecimal(unit[50]),
            //    //    IVA = Convert.ToDecimal(unit[51]),
            //    //    CodIVA = unit[52],
            //    //    IVASinSeguro = Convert.ToDecimal(unit[53]),
            //    //    TipoBulto = unit[54],
            //    //    CodBultos = unit[55],
            //    //    CantBulto = unit[56],
            //    //    Valor178 = Convert.ToDecimal(unit[57]),
            //    //    Valor191 = Convert.ToDecimal(unit[58]),
            //    //    CodImpto = unit[59],
            //    //    Impuesto = Convert.ToDecimal(unit[60]),
            //    //    CodAlmacen = unit[61],
            //    //    Valoralmacenaje = Convert.ToDecimal(unit[62]),
            //    //    CodCuenta = unit[63],
            //    //    Impto = unit[64],
            //    //    CodImptoAdicional = unit[65],
            //    //    PorcentajeImpuesto = unit[66],
            //    //    ValorImpuesto = Convert.ToDecimal(unit[67]),
            //    //    Despachador = unit[68],
            //    //    Aduana = unit[69],
            //    //    TipoOperacion = unit[70],
            //    //    Inspeccion = unit[71],
            //    //    CodInspecc = unit[72],
            //    //    DatoInterno = unit[73],
            //    //    UnidadMedida = unit[74],
            //    //    Dolar = Convert.ToDecimal(unit[75]),
            //    //    Valor91 = Convert.ToDecimal(unit[76]),
            //    //    Comentario1 = unit[77],
            //    //    Comentario2 = unit[78],
            //    //    Comentario3 = unit[79],
            //    //    Atributo = string.IsNullOrEmpty(unit[80]) ? "" : unit[80],
            //    //    Atributo2 = string.IsNullOrEmpty(unit[81]) ? "" : unit[81],
            //    //    Atributo3 = string.IsNullOrEmpty(unit[82]) ? "" : unit[82],
            //    //    Atributo4 = string.IsNullOrEmpty(unit[83]) ? "" : unit[83],
            //    //    Atributo5 = string.IsNullOrEmpty(unit[84]) ? "" : unit[84],
            //    //    Atributo6 = string.IsNullOrEmpty(unit[85]) ? "" : unit[85],
            //    //    DatoInternoAtrex = string.IsNullOrEmpty(unit[86]) ? "" : unit[86],
            //    //    DatointernoVacio = string.IsNullOrEmpty(unit[87]) ? "" : unit[87],
            //    //    FechaConfeccion = string.IsNullOrEmpty(unit[88]) ? DateTime.MinValue : DateTime.ParseExact(unit[88], "dd-MM-yyyy H:mm:ss", null),
            //    //    ArancelTratado = string.IsNullOrEmpty(unit[89]) ? "" : unit[89],
            //    //    CodigoAcuerdo = string.IsNullOrEmpty(unit[90]) ? "" : unit[90],
            //    //    GuiasAsociadas = string.IsNullOrEmpty(unit[91]) ? "" : unit[91],
            //    //});
            //}

            return listResponse;
        }

        private string FormatDatetime(string v)
        {
            var res = v;
            if (v.Length == 8)
            {
                res= string.Concat(v.Substring(0, 2), "-", v.Substring(2, 2), "-", v.Substring(4, 4));
            }
            return res;
        }

        private void ReadEnviromentVariables()
        {
            _synapseConnectionString = Environment.GetEnvironmentVariable("SynapConnectionString");
            _sorterConnectionString = Environment.GetEnvironmentVariable("SorterConnectionString");
            _insertTotal = Convert.ToInt32(Environment.GetEnvironmentVariable("TotalInsert"));
        }
    }
}
