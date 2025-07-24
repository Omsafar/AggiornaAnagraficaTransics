// Program.cs – Sync automatico anagrafica mezzi Transics <-> Gestionale
// Basato su StoredProcedure Stp_AnagraficaMezziUnica
// Include gestione duplicati: mantieni master (ID più alto), disattiva orfani
// © Paratori 2025

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Paratori.SyncTransics.TxTango;
using TxTango; // proxy SOAP generato da svcutil
using System.Net;
using System.Net.Mail;

namespace Paratori.SyncTransics
{
    internal static class Program
    {
        private const string ConnectionString =
            "Server=192.168.1.24\\sgam;Database=PARATORI;User Id=sapara;Password=S@p4ra;Encrypt=True;TrustServerCertificate=True;";
        private static readonly TimeSpan ApiCooldown = TimeSpan.FromSeconds(3);
        private static DateTime _lastApiCallUtc = DateTime.MinValue;
        private static readonly List<string> _changes = new();

        public static async Task Main(string[] args)
        {
            try
            {
                LogHelper.Log("=== Avvio sincronizzazione Transics ===");
                var oggi = DateTime.Today;

                // 1) VEICOLI GESTIONALE
                var gestList = LoadGestionale(oggi)
                    .OrderBy(v => v.Codice)
                    .ToList();
                Console.WriteLine("Veicoli recuperati dal gestionale:");
                foreach (var v in gestList)
                    Console.WriteLine($"  • Codice: {v.Codice}, Targa: {v.Targa}, Vettore: {v.Vettore}");
                Console.WriteLine($"Totale veicoli: {gestList.Count}\n");

                var gestDuplicati = gestList
                    .GroupBy(v => v.Codice, StringComparer.OrdinalIgnoreCase)
                    .Where(g => g.Count() > 1);
                if (gestDuplicati.Any())
                {
                    Console.WriteLine("❗ Duplicati in gestionale:");
                    foreach (var g in gestDuplicati)
                    {
                        Console.WriteLine($"  • '{g.Key}' x{g.Count()} volte:");
                        foreach (var vv in g)
                            Console.WriteLine($"     – Targa: {vv.Targa}, Vettore: {vv.Vettore}");
                    }
                    Console.WriteLine();
                }
                var gestVeicoli = gestList
                    .GroupBy(v => v.Codice, StringComparer.OrdinalIgnoreCase)
                    .Select(g => g.First())
                    .ToDictionary(v => v.Codice, StringComparer.OrdinalIgnoreCase);

                // 2) VEICOLI TRANSICS
                await WaitCooldown();
                var txStatusRes = await TransicsService.Client.Get_VehicleStatusAsync(
                    TransicsService.Login,
                    new VehicleStatusSelection { IncludeInactive = true, Identifiers = Array.Empty<IdentifierVehicle>() });
                var txVeicoli = txStatusRes.VehicleStatuses?.ToList() ?? new List<VehicleStatusResult>();

                // 2.b) GESTIONE DUPLICATI TRANSCICS
                var dupTxGroups = txVeicoli
                    .GroupBy(v => (v.VehicleCode ?? string.Empty).Trim(), StringComparer.OrdinalIgnoreCase)
                    .Where(g => g.Count() > 1);
                foreach (var grp in dupTxGroups)
                {
                    var code = grp.Key;
                    var items = grp.ToList();
                    bool existsInGest = gestVeicoli.ContainsKey(code);
                    var master = items.OrderBy(v => v.VehicleTransicsID).Last();

                    if (existsInGest)
                    {
                        if (!master.IsActive)
                        {
                            var d = $"Codice: {code}\nID master da attivare: {master.VehicleTransicsID}\nTarga: {master.LicensePlate}";
                            if (AskConfirmation("Attivazione duplicato", d))
                                await UpdateById(master.VehicleTransicsID, inactive: false);
                        }
                        foreach (var dup in items.Where(x => x.VehicleTransicsID != master.VehicleTransicsID && x.IsActive))
                        {
                            var d2 = $"Codice: {code}\nID da disattivare: {dup.VehicleTransicsID}\nTarga: {dup.LicensePlate}";
                            if (AskConfirmation("Disattivazione duplicato", d2))
                                await UpdateById(dup.VehicleTransicsID, inactive: true);
                        }
                    }
                    else
                    {
                        foreach (var dup in items.Where(x => x.IsActive))
                        {
                            var d3 = $"Codice: {code}\nID da disattivare: {dup.VehicleTransicsID}\nTarga: {dup.LicensePlate}";
                            if (AskConfirmation("Disattivazione orfani Transics", d3))
                                await UpdateById(dup.VehicleTransicsID, inactive: true);
                        }
                    }
                }

                var txByCode = txVeicoli
                    .GroupBy(v => (v.VehicleCode ?? string.Empty).Trim(), StringComparer.OrdinalIgnoreCase)
                    .Select(g => g.First())
                    .ToDictionary(v => (v.VehicleCode ?? string.Empty).Trim(), StringComparer.OrdinalIgnoreCase);
                LogHelper.Log($"Transics: {txByCode.Count} veicoli (dopo pulizia duplicati)");

                // 3) COMPANY CARDS
                await WaitCooldown();
                var cardsRes = await TransicsService.Client.Get_CompanyCardsAsync(TransicsService.Login);
                var companyCards = cardsRes.CompanyCardResults?.Select(c => c.CardName)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase) ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                // 4) DIFFERENZE
                var gestCodes = new HashSet<string>(gestVeicoli.Keys, StringComparer.OrdinalIgnoreCase);
                var txCodes = new HashSet<string>(txByCode.Keys, StringComparer.OrdinalIgnoreCase);
                var daCreare = gestCodes.Except(txCodes).ToList();
                var daDisattivare = txCodes.Except(gestCodes).ToList();
                var daAggiornare = gestCodes.Intersect(txCodes).ToList();
                LogHelper.Log($"Creare: {daCreare.Count} | Disattivare: {daDisattivare.Count} | Aggiornare: {daAggiornare.Count}");

                // 5) DISATTIVAZIONI
                foreach (var cod in daDisattivare)
                {
                    var t = txByCode[cod];
                    var d = $"Codice: {cod}\nID: {t.VehicleTransicsID}\nTarga: {t.LicensePlate}";
                    if (t.IsActive && AskConfirmation("Disattivazione veicolo", d))
                        await UpdateById(t.VehicleTransicsID, inactive: true);

                }
                // 6) CREAZIONI
                foreach (var cod in daCreare)
                {
                    var g = gestVeicoli[cod];
                    var d = $"Codice: {g.Codice}\nTarga: {g.Targa}\nVettore: {g.Vettore}";
                    if (AskConfirmation("Creazione veicolo", d))
                        await InsertVehicle(g, companyCards);
                }
                // 7) AGGIORNAMENTI
                foreach (var cod in daAggiornare)
                {
                    var g = gestVeicoli[cod];
                    var t = txByCode[cod];
                    var changes = new List<string>();
                    if (!t.LicensePlate.Equals(g.Targa, StringComparison.OrdinalIgnoreCase))
                        changes.Add($"Targa: '{t.LicensePlate}' → '{g.Targa}'");
                    if (!t.VehicleExternalCode.Equals(g.Targa, StringComparison.OrdinalIgnoreCase))
                        changes.Add($"ExternalCode: '{t.VehicleExternalCode}' → '{g.Targa}'");
                    if (changes.Count == 0) continue;

                    var d = string.Join("\n", changes);
                    if (AskConfirmation("Aggiornamento veicolo", d))
                        await UpdateVehicle(g, t, companyCards);
                }

                LogHelper.Log("=== Sincronizzazione completata ===");
                if (_changes.Count > 0)
                    SendSummaryEmail("Modifiche eseguite:\n" + string.Join("\n", _changes));
            }
            catch (Exception ex)
            {
                LogHelper.LogException(ex);
                SendErrorEmail(ex);
            }
        }

        // Helper interattivo
        private static bool AskConfirmation(string action, string details)
        {
            Console.WriteLine($">>> {action}");
            Console.WriteLine(details);
            Console.Write("Confermi? (S/N): ");
            return Console.ReadLine()?.Trim().Equals("S", StringComparison.OrdinalIgnoreCase) == true;
        }

        // CRUD Transics by code
        private static async Task DisableVehicle(string code)
        {
            await WaitCooldown();
            var upd = new VehicleUpdate
            {
                VehicleToUpdate = new IdentifierVehicle { IdentifierVehicleType = enumIdentifierVehicleType.CODE, Id = code },
                Inactive = true
            };
            var res = await TransicsService.Client.Update_Vehicle_V2Async(TransicsService.Login, upd);
            LogHelper.LogOutcome($"DISATTIVA {code}", res);
            RecordChange($"Disattivato veicolo {code}");
        }
        /// <summary>
        /// Crea un nuovo veicolo in Transics, impostando CompanyCard e FuelType nel blocco TechnicalInfo.
        /// </summary>
        private static async Task InsertVehicle(Veicolo g, HashSet<string> cards)
        {
            // Rispetta il cooldown fra chiamate API
            await WaitCooldown();

            // 1) Mappa il carburante dal gestionale all’enum Transics (FuelType è su VehicleTechnicalInfo) :contentReference[oaicite:0]{index=0}:contentReference[oaicite:1]{index=1}
            var fuel = MapFuelType(g.TipoCarburante);

            // 2) Costruisci l’oggetto VehicleInsert
            var ins = new VehicleInsert
            {
                VehicleID = g.Codice,
                LicensePlate = g.Targa,
                VehicleExternalCode = g.Targa,
                Inactive = false,
                CompanyCard = MapCompanyCard(g.Vettore, cards),

                // 3) Assegna FuelType dentro TechnicalInfo
                TechnicalInfo = new VehicleTechnicalInfo
                {
                    FuelType = fuel,
                    FuelTypeSpecified = fuel.HasValue
                }
            };

            // 4) Chiamata SOAP per l’inserimento
            var res = await TransicsService.Client.Insert_VehicleAsync(TransicsService.Login, ins);

            // 5) Logga l’esito
            LogHelper.LogOutcome($"CREA {g.Codice}", res);
            RecordChange($"Creato veicolo {g.Codice} targa {g.Targa}");
        }


        private static async Task UpdateVehicle(Veicolo g, VehicleStatusResult t, HashSet<string> cards)
        {
            var upd = new VehicleUpdate
            {
                VehicleToUpdate = new IdentifierVehicle { IdentifierVehicleType = enumIdentifierVehicleType.CODE, Id = g.Codice }
            };
            bool dirty = false;
            if (!t.LicensePlate.Equals(g.Targa, StringComparison.OrdinalIgnoreCase)) { upd.LicensePlate = g.Targa; dirty = true; }
            if (!t.VehicleExternalCode.Equals(g.Targa, StringComparison.OrdinalIgnoreCase)) { upd.VehicleExternalCode = g.Targa; dirty = true; }
            if (!dirty) return;
            upd.CompanyCard = MapCompanyCard(g.Vettore, cards);

            await WaitCooldown();
            var res = await TransicsService.Client.Update_Vehicle_V2Async(TransicsService.Login, upd);
            LogHelper.LogOutcome($"UPDATE {g.Codice}", res);
            RecordChange($"Aggiornato veicolo {g.Codice} targa {g.Targa}");
        }
        // CRUD Transics by internal ID
        private static async Task UpdateById(long id, bool inactive)
        {
            await WaitCooldown();
            var upd = new VehicleUpdate
            {
                VehicleToUpdate = new IdentifierVehicle
                {
                    IdentifierVehicleType = enumIdentifierVehicleType.TRANSICS_ID,
                    Id = id.ToString()
                },

                Inactive = inactive
            };
            var res = await TransicsService.Client.Update_Vehicle_V2Async(TransicsService.Login, upd);
            var act = inactive ? "DISATTIVA" : "ATTIVA";
            LogHelper.LogOutcome($"{act} ID={id}", res);
            RecordChange($"{act} veicolo ID={id}");
        }

        // DB
        private static IEnumerable<Veicolo> LoadGestionale(DateTime giorno)
        {
            using var conn = new SqlConnection(ConnectionString);
            using var cmd = new SqlCommand("Stp_AnagraficaMezziUnica", conn) { CommandType = CommandType.StoredProcedure };
            cmd.Parameters.AddWithValue("@Tipo", 0);
            cmd.Parameters.AddWithValue("@MostraTabella", 1);
            cmd.Parameters.AddWithValue("@Data", giorno.ToString("yyyyMMdd"));
            cmd.Parameters.AddWithValue("@Alienati", "N");
            conn.Open();
            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                var codice = rd["CODICE"]?.ToString()?.Trim();
                if (string.IsNullOrEmpty(codice)) continue;

                var targa = rd["Targa"]?.ToString()?.Trim() ?? string.Empty;
                var vettore = rd["Vettore"]?.ToString() ?? string.Empty;
                var tipoCarb = rd["TIPO_CARBURANTE"]?.ToString()?.Trim() ?? string.Empty;

                yield return new Veicolo(codice, targa, vettore, tipoCarb);
            }

        }

        // Utility
        private static async Task WaitCooldown()
        {
            var diff = DateTime.UtcNow - _lastApiCallUtc;
            if (diff < ApiCooldown) await Task.Delay(ApiCooldown - diff);
            _lastApiCallUtc = DateTime.UtcNow;
        }
        private static string MapCompanyCard(string vettore, HashSet<string> cards)
        {
            if (string.IsNullOrWhiteSpace(vettore)) return null;
            var up = vettore.ToUpperInvariant();
            if (up.Contains("PARATORI") && cards.Contains("Paratori SPA")) return "Paratori SPA";
            if (up.Contains("SISTEMI") && cards.Contains("SistemiTrasporti SR")) return "SistemiTrasporti SR";
            return null;
        }

        /// <summary>
        /// Converte il codice gestionale in FuelType Transics (enum in Vehicle_V2).
        /// </summary>
        private static FuelType? MapFuelType(string tipo)
        {
            if (string.IsNullOrWhiteSpace(tipo)) return null;
            switch (tipo.Trim().ToUpperInvariant())
            {
                case "Gasolio":    // Diesel
                    return FuelType.Diesel_l;
                case "Metano Lng":
                    return FuelType.LNG_l;
                default:
                    return null;

            }
        }

        private static void RecordChange(string msg) => _changes.Add(msg);

        private static void SendSummaryEmail(string body)
        {
            using var message = new MailMessage();
            message.From = new MailAddress("AnagraficaTransics@paratorispa.it");
            message.To.Add("omar.tagliabue@paratorispa.it");
            message.Subject = "AggiornaAnagraficaGolia - Modifiche eseguite";
            message.Body = body;

            using var smtp = new SmtpClient("192.168.1.11");
            smtp.Credentials = new NetworkCredential("scanner@paraspa.local", "],M4`V~8q967");
            smtp.Send(message);
        }

        private static void SendErrorEmail(Exception ex)
        {
            using var message = new MailMessage();
            message.From = new MailAddress("AnagraficaTransics@paratorispa.it");
            message.To.Add("omar.tagliabue@paratorispa.it");
            message.Subject = "Errore AggiornaAnagraficaGolia";
            message.Body = ex.ToString();

            using var smtp = new SmtpClient("192.168.1.11");
            smtp.Credentials = new NetworkCredential("scanner@paraspa.local", "],M4`V~8q967");
            smtp.Send(message);
        }


        private record Veicolo(
          string Codice,
          string Targa,
          string Vettore,
          string TipoCarburante
         );
    }


    // Logging
    internal static class LogHelper
    {
        public static void Log(string msg)
            => Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {msg}");
        public static void LogException(Exception ex)
            => Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] ERRORE: {ex}");
        public static void LogOutcome(string ctx, ResultInfo res)
            => Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {ctx}: {(res?.Errors?.Length > 0 ? $"ERROR {res.Errors[0].ErrorCode}" : "OK")}");
    }

    // Extension TransicsService
    namespace TxTango
    {
        public static partial class TransicsService
        {
            private static readonly ServiceSoapClient _client =
                new ServiceSoapClient(ServiceSoapClient.EndpointConfiguration.ServiceSoap12);
            private static readonly Login _loginBlock = new Login
            {
                Dispatcher = "PARATORI",
                Password = "PARATORI_3278800362",
                Language = "EN",
                Integrator = "PARATORI",
                SystemNr = 362
            };
            public static ServiceSoapClient Client => _client;
            public static Login Login => _loginBlock;
        }
    }
}
