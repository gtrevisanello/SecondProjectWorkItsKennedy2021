using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;
using System.Text;
using ITSOPCCourseCode.OPCUA.SampleClient.Services;
using Opc.Ua;

namespace Data_transmitter
{
    public interface IValidable
    {
        public bool isValid();
    }
    class Rilevazione : IValidable
    {
        public Commessa Commessa { get; set; }
        public SnapshotMacchinario Macchina { get; set; }
        public DateTime Timestamp { get; set; }
        public Int32 Pezzi_buoni_prodotti { get; set; }
        public Int32 Pezzi_scarti_prodotti { get; set; }
        public bool isValid()
        {
            return this.Commessa.isValid() && this.Macchina.isValid() && this.Timestamp != null;
        }
    }
    class Commessa : IValidable
    {
        public Int32 ID_commessa { get; set; }
        public Int32 ID_prodotto { get; set; }
        public Int32 Pezzi_totali { get; set; }

        public bool isValid()
        {
            return this.ID_commessa != null && this.ID_prodotto != null && this.Pezzi_totali != null;
        }
    }
    class SnapshotMacchinario : IValidable
    {
        public enum Stato_macchina
        {
            Attiva,
            Errore,
            Stop
        }
        public Int32 ID_macchinario { get; set; }
        public TimeSpan Ore_di_lavoro { get; set; }
        public Int32[] Numero_attivazioni_pistoni { get; set; }
        public Int32[] ID_pistoni { get; set; }
        public Int32[] Numero_attivazioni_sensori { get; set; }
        public Int32[] ID_sensori { get; set; }
        public Stato_macchina Stato { get; set; }
        public Int32 Pezzi_al_minuto { get; set; }
        public bool[] Allarmi { get; set; } //Massimo 80 allarmi

        public bool isValid()
        {
            return ID_macchinario != null;
        }
    }
    class AzureSQLAdapter
    {
        private static string connection_string = "Server=tcp:my-scatamburlo.database.windows.net,1433;Initial Catalog=project-work;Persist Security Info=False;User ID=db_root;Password=pippo1234!;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
        public static void carica_commessa(Commessa comm)
        {
            // TODO : Creare stored_procedure Upsert_commessa
            string command_string = "EXEC Upsert_commessa @ID_commessa = @idComm, @ID_prodotto = @idProd, @Pezzi_totali = @pzTot";
            SqlConnection conn = new SqlConnection(connection_string);
            SqlCommand cmd = new SqlCommand(command_string, conn);
            cmd.Parameters.Add("@idComm", SqlDbType.Int);
            cmd.Parameters["@idComm"].Value = comm.ID_commessa;
            cmd.Parameters.Add("@idProd", SqlDbType.Int);
            cmd.Parameters["@idProd"].Value = comm.ID_prodotto;
            cmd.Parameters.Add("@pzTot", SqlDbType.Int);
            cmd.Parameters["@pzTot"].Value = comm.Pezzi_totali;
            try {
                conn.Open();
                cmd.ExecuteNonQuery();
            } catch (Exception exc) 
            { 
                
            } finally {
                conn.Close();
            }
        }
        public static void carica_rilevazioni(Rilevazione[] pezzi)
        {
            SqlConnection conn = new SqlConnection(connection_string);
            
            string string_cmd_rilevazioni = "INSERT INTO tblRilevazione (ID_commessa, ID_macchinario, Timestamp, Pezzi_buoni_prodotti, Pezzi_scarti_prodotti)" +
                "VALUES ";
            string string_cmd_snapshot_macchinario = "INSERT INTO tblSnapshotMacchinario (ID_macchinario, Stato_macchina, Ore_di_lavoro, Numero_attivazioni_pistoni, Numero_attivazioni_sensori, Pezzi_al_minuto, Timestamp, Allarmi)" +
                "VALUES ";
            string string_cmd_snapshot_sensori_ed_attuatori = "INSERT INTO logicRilevazioniStatoSensori (ID_macchina, ID_componente, Numero_attivazioni, ID_rilevazione)" +
                "VALUES ";
            
            SqlCommand cmd_rilevazioni = new SqlCommand(), cmd_snapshot_macchinario = new SqlCommand(), cmd_snapshot_sensori_ed_attuatori = new SqlCommand();
            cmd_rilevazioni.Connection = cmd_snapshot_macchinario.Connection = cmd_snapshot_sensori_ed_attuatori.Connection = conn;

            int counter = 0; //  Counter per la generazione dei nomi dei parametri
            foreach (Rilevazione pz in pezzi)
            {
                if (!pz.isValid())
                    throw new ArgumentException("Uno dei pezzi inseriti non era valido");

                // Query per le rilevazioni
                string_cmd_rilevazioni += $"(@ID_commessa{counter}, @ID_macchinario{counter}, @Timestamp{counter}, @Pezzi_buoni_prodotti{counter}, @Pezzi_scarti_prodotti{counter}),";


                cmd_rilevazioni.Parameters.Add($"@ID_commessa{counter}", SqlDbType.Int);
                cmd_rilevazioni.Parameters.Add($"@ID_macchinario{counter}", SqlDbType.Int);
                cmd_rilevazioni.Parameters.Add($"@Timestamp{counter}", SqlDbType.Timestamp);
                cmd_rilevazioni.Parameters.Add($"@Pezzi_buoni_prodotti{counter}", SqlDbType.Int);
                cmd_rilevazioni.Parameters.Add($"@Pezzi_scarti_prodotti{counter}", SqlDbType.Int);

                cmd_rilevazioni.Parameters[$"@ID_commessa{counter}"].Value = pz.Commessa.ID_commessa;
                cmd_rilevazioni.Parameters[$"@ID_macchinario{counter}"].Value = pz.Macchina.ID_macchinario;
                cmd_rilevazioni.Parameters[$"@Timestamp{counter}"].Value = pz.Timestamp;
                cmd_rilevazioni.Parameters[$"@Pezzi_buoni_prodotti{counter}"].Value = pz.Pezzi_buoni_prodotti;
                cmd_rilevazioni.Parameters[$"@Pezzi_scarti_prodotti{counter}"].Value =pz.Pezzi_scarti_prodotti;

                // Query per lo stato generico del macchinario
                string_cmd_snapshot_macchinario += $"(@ID_macchinario{counter}, (SELECT TOP 1 ID_stato FROM tblStatiMacchinario WHERE Descrizione = @Stato_macchina{counter}), @Ore_di_lavoro{counter}, @Numero_attivazioni_pistoni{counter}, @Numero_attivazioni_sensori{counter}, @Pezzi_al_minuto{counter}, @Timestamp{counter}, @Allarmi{counter}),";

                cmd_snapshot_macchinario.Parameters.Add($"@ID_macchinario{counter}", SqlDbType.Int);
                cmd_snapshot_macchinario.Parameters.Add($"@Stato_macchina{counter}", SqlDbType.VarChar);
                cmd_snapshot_macchinario.Parameters.Add($"@Ore_di_lavoro{counter}", SqlDbType.BigInt);
                cmd_snapshot_macchinario.Parameters.Add($"@Numero_attivazioni_pistoni{counter}", SqlDbType.Int);
                cmd_snapshot_macchinario.Parameters.Add($"@Numero_attivazioni_sensori{counter}", SqlDbType.Int);
                cmd_snapshot_macchinario.Parameters.Add($"@Pezzi_al_minuto{counter}", SqlDbType.Int);
                cmd_snapshot_macchinario.Parameters.Add($"@Timestamp{counter}", SqlDbType.Timestamp);
                cmd_snapshot_macchinario.Parameters.Add($"@Allarmi{counter}", SqlDbType.Binary);

                cmd_snapshot_macchinario.Parameters[$"@ID_macchinario{counter}"].Value = pz.Macchina.ID_macchinario;
                cmd_snapshot_macchinario.Parameters[$"@Stato_macchina{counter}"].Value = pz.Macchina.Stato.ToString();
                cmd_snapshot_macchinario.Parameters[$"@Ore_di_lavoro{counter}"].Value = pz.Macchina.Ore_di_lavoro.Ticks;
                cmd_snapshot_macchinario.Parameters[$"@Numero_attivazioni_pistoni{counter}"].Value = pz.Macchina.Numero_attivazioni_pistoni;
                cmd_snapshot_macchinario.Parameters[$"@Numero_attivazioni_sensori{counter}"].Value = pz.Macchina.Numero_attivazioni_sensori;
                cmd_snapshot_macchinario.Parameters[$"@Pezzi_al_minuto{counter}"].Value = pz.Macchina.Pezzi_al_minuto;
                cmd_snapshot_macchinario.Parameters[$"@Timestamp{counter}"].Value = pz.Timestamp;
                cmd_snapshot_macchinario.Parameters[$"@Allarmi{counter}"].Value = pz.Macchina.Allarmi;

                // Query per la memorizzazione degli stati dei sensori

                cmd_snapshot_sensori_ed_attuatori.Parameters.Add($"@ID_macchinario{counter}", SqlDbType.Int);
                cmd_snapshot_sensori_ed_attuatori.Parameters.Add($"@ID_commessa{counter}", SqlDbType.Int);
                cmd_snapshot_sensori_ed_attuatori.Parameters.Add($"@Timestamp{counter}", SqlDbType.Timestamp);

                cmd_snapshot_sensori_ed_attuatori.Parameters[$"@ID_macchinario{counter}"].Value = pz.Macchina.ID_macchinario;
                cmd_snapshot_sensori_ed_attuatori.Parameters[$"@ID_commessa{counter}"].Value = pz.Commessa.ID_commessa;
                cmd_snapshot_sensori_ed_attuatori.Parameters[$"@Timestamp{counter}"].Value = pz.Timestamp;
                for (int numero_sensore = 0; numero_sensore < pz.Macchina.Numero_attivazioni_sensori.Length; numero_sensore++)
                {
                    string_cmd_snapshot_sensori_ed_attuatori += $"(@ID_macchinario{counter}, @ID_sensore{counter}_{numero_sensore}, @Numero_attivazioni{counter}_{numero_sensore}, (SELECT TOP 1 ID_rilevazione FROM tblRilevazione WHERE ID_commessa = @ID_commessa{counter} AND ID_macchinario = @ID_macchinario{counter} AND Timestamp = @Timestamp{counter})),";

                    cmd_snapshot_sensori_ed_attuatori.Parameters.Add($"@ID_sensore{counter}_{numero_sensore}", SqlDbType.Int);
                    cmd_snapshot_sensori_ed_attuatori.Parameters.Add($"@Numero_attivazioni{counter}_{numero_sensore}", SqlDbType.Int);

                    cmd_snapshot_sensori_ed_attuatori.Parameters[$"@ID_sensore{counter}_{numero_sensore}"].Value = pz.Macchina.ID_sensori[numero_sensore];
                    cmd_snapshot_sensori_ed_attuatori.Parameters[$"@Numero_attivazioni{counter}_{numero_sensore}"].Value = pz.Macchina.Numero_attivazioni_sensori[numero_sensore];
                }
                for (int numero_attuatore = 0; numero_attuatore < pz.Macchina.Numero_attivazioni_pistoni.Length; numero_attuatore++)
                {
                    string_cmd_snapshot_sensori_ed_attuatori += $"(@ID_macchinario{counter}, @ID_attuatore{counter}_{numero_attuatore}, @Numero_attivazioni{counter}_{numero_attuatore}, (SELECT TOP 1 ID_rilevazione FROM tblRilevazione WHERE ID_commessa = @ID_commessa{counter} AND ID_macchinario = @ID_macchinario{counter} AND Timestamp = @Timestamp{counter})),";

                    cmd_snapshot_sensori_ed_attuatori.Parameters.Add($"@ID_attuatore{counter}_{numero_attuatore}", SqlDbType.Int);
                    cmd_snapshot_sensori_ed_attuatori.Parameters.Add($"@Numero_attivazioni_attuatore{counter}_{numero_attuatore}", SqlDbType.Int);

                    cmd_snapshot_sensori_ed_attuatori.Parameters[$"@ID_attuatore{counter}_{numero_attuatore}"].Value = pz.Macchina.ID_sensori[numero_attuatore];
                    cmd_snapshot_sensori_ed_attuatori.Parameters[$"@Numero_attivazioni_attuatore{counter}_{numero_attuatore}"].Value = pz.Macchina.Numero_attivazioni_sensori[numero_attuatore];
                }



                counter++;
            }

            string_cmd_rilevazioni = string_cmd_rilevazioni.Remove(string_cmd_rilevazioni.Length - 1); //Rimuovo la virgola in eccesso alla fine dela query
            string_cmd_snapshot_macchinario = string_cmd_snapshot_macchinario.Remove(string_cmd_snapshot_macchinario.Length - 1);
            string_cmd_snapshot_sensori_ed_attuatori = string_cmd_snapshot_sensori_ed_attuatori.Remove(string_cmd_snapshot_sensori_ed_attuatori.Length - 1);

            cmd_rilevazioni.CommandType = cmd_snapshot_macchinario.CommandType = cmd_snapshot_sensori_ed_attuatori.CommandType = CommandType.Text;
            
            cmd_rilevazioni.CommandText = string_cmd_rilevazioni;
            cmd_snapshot_macchinario.CommandText = string_cmd_snapshot_macchinario;
            cmd_snapshot_sensori_ed_attuatori.CommandText = string_cmd_snapshot_sensori_ed_attuatori;

            Console.WriteLine(cmd_snapshot_macchinario.CommandText);
            
            try
            {
                conn.Open();
                cmd_rilevazioni.ExecuteNonQuery();
                cmd_snapshot_macchinario.ExecuteNonQuery();
                cmd_snapshot_sensori_ed_attuatori.ExecuteNonQuery();
            }
            catch (Exception exc)
            {
                Console.WriteLine("Problemi nella query : " + exc.Message);
            }
            finally
            { conn.Close(); }
            
        }
    }

    class OPCUADataCollector
    {
        public string uri_string = "opc.tcp://192.168.1.71:4840";
        private PlcCommunicationService communicationService;
        Dictionary<string, string> indirizzi_dati;
        public List<Rilevazione> rilevazioni_memorizzate;
        string known_root_node = "ns=4;i=2";

        public OPCUADataCollector()
        {
            rilevazioni_memorizzate = new List<Rilevazione>();
            this.communicationService = new PlcCommunicationService();
            communicationService.StartAsync(uri_string).GetAwaiter().GetResult();
            indirizzi_dati = new Dictionary<string, string>
            {
                {"ID_commessa",  getNodeIDByName("ID_commessa", known_root_node)},
                {"ID_macchinario", getNodeIDByName("ID_macchinario", known_root_node) },
                {"ID_prodotto", getNodeIDByName("ID_prodotto", known_root_node) },
                {"Pezzi_totali", getNodeIDByName("Pezzi_totali", known_root_node) },
                {"Timestamp", getNodeIDByName("Timestamp", known_root_node) },
                {"Pezzi_buoni_prodotti", getNodeIDByName("Pezzi_buoni_prodotti", known_root_node) },
                {"Pezzi_scarti_prodotti", getNodeIDByName("Pezzi_scarti_prodotti", known_root_node) },
                {"Stato_macchina", getNodeIDByName("Stato_macchina", known_root_node) },
                {"Ore_di_lavoro", getNodeIDByName("Ore_di_lavoro", known_root_node) },
                {"ID_pistoni", getNodeIDByName("ID_pistoni", known_root_node) },
                {"Numero_attivazioni_pistoni", getNodeIDByName("Numero_attivazioni_pistoni", known_root_node) },
                {"ID_sensori", getNodeIDByName("ID_sensori", known_root_node) },
                {"Numero_attivazioni_sensori", getNodeIDByName("Numero_attivazioni_sensori", known_root_node) },
                {"Pezzi_al_minuto", getNodeIDByName("Pezzi_al_minuto", known_root_node) },
                {"Allarmi", getNodeIDByName("Allarmi", known_root_node) }
            };
            // Creo un subscribe per cui raccolgo i dati solamente quando sono variati i pezzi prodotti (ed ogni due secondi)
            communicationService.SubscribeToNodeChanges(new List<string>() { indirizzi_dati["Pezzi_buoni_prodotti"], indirizzi_dati["Pezzi_scarti_prodotti"] }, 2000);
            communicationService.NodeValueChanged += get_data;
        }
        public Rilevazione get_data()
        {
            Rilevazione dato = new Rilevazione();
            dato.Commessa = new Commessa();
            dato.Macchina = new SnapshotMacchinario();

            var communication_service = new PlcCommunicationService();

            dato.Commessa.ID_commessa = Convert.ToInt32(communicationService.ReadNodeValue(indirizzi_dati["ID_commessa"]));
            dato.Commessa.ID_prodotto = Convert.ToInt32(communicationService.ReadNodeValue(indirizzi_dati["ID_prodotto"]));
            dato.Commessa.Pezzi_totali = Convert.ToInt32(communicationService.ReadNodeValue(indirizzi_dati["Pezzi_totali"]));

            dato.Macchina.ID_macchinario = Convert.ToInt32(communicationService.ReadNodeValue(indirizzi_dati["ID_macchinario"]));
            dato.Macchina.Numero_attivazioni_pistoni = communicationService.ReadNodeValue(indirizzi_dati["Numero_attivazioni_pistoni"]) as Int32[];
            dato.Macchina.ID_pistoni = communicationService.ReadNodeValue(indirizzi_dati["ID_pistoni"]) as Int32[];
            dato.Macchina.Numero_attivazioni_sensori = communicationService.ReadNodeValue(indirizzi_dati["Numero_attivazioni_sensori"]) as Int32[];
            dato.Macchina.ID_sensori = communicationService.ReadNodeValue(indirizzi_dati["ID_sensori"]) as Int32[];
            dato.Macchina.Ore_di_lavoro = TimeSpan.FromTicks(Convert.ToInt64(communicationService.ReadNodeValue(indirizzi_dati["Ore_di_lavoro"])));
            dato.Macchina.Pezzi_al_minuto = Convert.ToInt32(communicationService.ReadNodeValue(indirizzi_dati["Pezzi_al_minuto"]));
            dato.Macchina.Stato = (SnapshotMacchinario.Stato_macchina)Enum.Parse(typeof(SnapshotMacchinario.Stato_macchina), Convert.ToString(communicationService.ReadNodeValue(indirizzi_dati["Stato_macchina"])));
            
            dato.Pezzi_buoni_prodotti = Convert.ToInt32(communicationService.ReadNodeValue(indirizzi_dati["Pezzi_buoni_prodotti"]));
            dato.Pezzi_scarti_prodotti = Convert.ToInt32(communicationService.ReadNodeValue(indirizzi_dati["Pezzi_scarti_prodotti"]));
            dato.Timestamp = Convert.ToDateTime(communicationService.ReadNodeValue(indirizzi_dati["Timestamp"]));

            return dato;
        }
        // Metodo utilizzato dall'event handler
        public void get_data(object sender, ITSOPCCourseCode.OPCUA.SampleClient.DTO.NodeValueChangedNotification e)
        {
            rilevazioni_memorizzate.Add(get_data());
        }
        public string getNodeIDByName( string name, string root_node)
        {
            if (communicationService.ReadNode(root_node).ToString() == name)
                return root_node;
            var node_list = communicationService.BrowseNode(root_node);
            if (node_list.Count == 0)
                return "";
            foreach (var item in node_list)
            {
                string ret = getNodeIDByName( item.NodeId.ToString(), name);
                if (ret != "")
                {
                    Console.WriteLine("Trovato! " + ret);
                    return ret;
                }
            }
            return "";
        }
    }
}
