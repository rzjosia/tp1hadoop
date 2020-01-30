package tp1hadoop;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.thriftfs.api.Datanode.Processor.readBlock;

public class Tp1 {

	Configuration conf;
	Table hTable;
	Connection con;
	Admin admin;

	/**
	 * Contructeur
	 */
	public Tp1() {

		System.out.println("init");
		init();

		System.out.println("create table");
		createTable();

		System.out.println("instance hbase");
		instanceHbase();

		System.out.println("add csv");
		addCSV();

		System.out.println("fermet instance hbase");
		fermerInstanceHbase();
	}

	/**
	 * Initialiser la configuration et objet admin Hbase
	 */
	public void init() {
		try {

			// Instance configuration
			conf = HBaseConfiguration.create();
			con = ConnectionFactory.createConnection();

			// Instancier la classe admin
			admin = con.getAdmin();

		} catch (ZooKeeperConnectionException ex) {
			Logger.getLogger(Tp1.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(Tp1.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	/**
	 * Créer la table
	 */
	public void createTable() {

		try {
			// Supprimer la table

			System.out.println("Supprimer la table courant");

			System.out.println("Disable PARIS:Arbres");
			admin.disableTables("PARIS:Arbres");

			System.out.println("Delete PARIS:Arbres");
			admin.deleteTables("PARIS:Arbres");
		} catch (Exception e) {
			System.out
					.println("Disable PARIS:Arbres impossible table n'existe pas...");
		}

		try {
			// Supprimer le namespace

			System.out.println("Supprimer le namespace");

			admin.deleteNamespace("PARIS");
		} catch (Exception e) {
			System.out
					.println("delete namespace HOPITAL impossible espace de nom encore utilisé ou inexistant...");
		}

		try {
			// Créer l'espace de nom

			System.out.println("Créer le namespace");

			NamespaceDescriptor.Builder builder = NamespaceDescriptor
					.create("PARIS");
			NamespaceDescriptor nsd = builder.build();
			admin.createNamespace(nsd);

		} catch (Exception e) {
			System.out
					.println("Erreur de création de l'espace de nom PARIS => existe déja...");
		}

		try {
			// Créer la table

			System.out.println("Créer à nouveau la table");

			HTableDescriptor tableDesc = new HTableDescriptor(
					TableName.valueOf("PARIS:Arbres"));

			// Ajouter les colonnes
			tableDesc.addFamily(new HColumnDescriptor("genre"));
			tableDesc.addFamily(new HColumnDescriptor("infos"));
			tableDesc.addFamily(new HColumnDescriptor("adresse"));

			admin.createTable(tableDesc);
			admin.close();

		} catch (Exception e) {
			System.out
					.println("Erreur de création de l'espace de nom PARIS => existe déja...");
		}

	}

	/**
	 * Ajouter les données CSV
	 */
	public void addCSV() {
		Reader in;
		ArrayList<Put> puts = new ArrayList<Put>();

		try {
			in = new FileReader("arbresremarquablesparis2011.csv");
			Iterable<CSVRecord> records = CSVFormat.EXCEL.parse(in);
			for (CSVRecord record : records) {
				String geoPoint = record.get(0);
				String genre = record.get(1);
				String espece = record.get(2);
				String adresse = record.get(3);
				String arrondissement = record.get(4);
				String circonference = record.get(5);
				String hauteur = record.get(6);
				String variete = record.get(7);
				String datePlantation = record.get(8);
				String objectID = record.get(9);

				/*
				 * System.out.println("geoPoint: " + geoPoint);
				 * System.out.println("genre: " + genre);
				 * System.out.println("espece: " + espece);
				 * System.out.println("adresse: " + adresse);
				 * System.out.println("arrondissement: " + arrondissement);
				 * System.out.println("circonference: " + circonference);
				 * System.out.println("hauteur: " + hauteur);
				 * System.out.println("variété: " + variete);
				 * System.out.println("date plantation: " + datePlantation);
				 * System.out.println("object id: " + objectID);
				 * System.out.println();
				 */

				Put p = new Put(Bytes.toBytes(objectID));

				// Colonne genre
				p.addColumn(Bytes.toBytes("genre"), Bytes.toBytes("genre"),
						Bytes.toBytes(genre));

				p.addColumn(Bytes.toBytes("genre"), Bytes.toBytes("espece"),
						Bytes.toBytes(espece));

				p.addColumn(Bytes.toBytes("genre"), Bytes.toBytes("variete"),
						Bytes.toBytes(variete));

				// Colonne infos
				p.addColumn(Bytes.toBytes("infos"),
						Bytes.toBytes("date_plantation"),
						Bytes.toBytes(datePlantation));

				p.addColumn(Bytes.toBytes("infos"), Bytes.toBytes("hauteur"),
						Bytes.toBytes(hauteur));

				p.addColumn(Bytes.toBytes("infos"),
						Bytes.toBytes("circonference"),
						Bytes.toBytes(circonference));

				// Colonne adresse
				p.addColumn(Bytes.toBytes("adresse"),
						Bytes.toBytes("geopoint"), Bytes.toBytes(geoPoint));

				p.addColumn(Bytes.toBytes("adresse"),
						Bytes.toBytes("arrondissement"),
						Bytes.toBytes(arrondissement));

				p.addColumn(Bytes.toBytes("adresse"), Bytes.toBytes("adresse"),
						Bytes.toBytes(adresse));

				puts.add(p);
			}

			hTable.put(puts);
			System.out.println("donnees inserees");
		} catch (Exception ex) {
			Logger.getLogger(Tp1.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	/**
	 * Instance HBASE
	 */
	public void instanceHbase() {
		try {

			// Instance HTable
			hTable = con.getTable(TableName.valueOf("PARIS:Arbres"));

		} catch (IOException ex) {
			Logger.getLogger(Tp1.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	/**
	 * Fermer instance HBase
	 */
	public void fermerInstanceHbase() {
		try {
			// fermer HTable
			hTable.close();
		} catch (IOException ex) {
			Logger.getLogger(Tp1.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	/**
	 * Requête HBASE
	 */
	public void operation() {
		try {
			String cle = "666";

			// Le genre de l'arbre 666
			Get g = new Get(Bytes.toBytes(cle));
			Result r = hTable.get(g);

			String genre = Bytes.toString(r.getValue(Bytes.toBytes("genre"),
					Bytes.toBytes("genre")));

			System.out
					.println("Le genre de l'arbre " + cle + " est : " + genre);

		} catch (IOException ex) {
			Logger.getLogger(Tp1.class.getName()).log(Level.SEVERE, null, ex);
		}

		System.out.println();

		try {
			// Afficher les valeurs de la famille « infos » de l’arbre
			// arbre-66300.

			String cle = "66300";

			Get g = new Get(Bytes.toBytes(cle));
			Result r = hTable.get(g.addFamily(Bytes.toBytes("infos")));

			String datePlantation = Bytes.toString(r.getValue(
					Bytes.toBytes("infos"), Bytes.toBytes("date_plantation")));

			String hauteur = Bytes.toString(r.getValue(Bytes.toBytes("infos"),
					Bytes.toBytes("hauteur")));

			String circonference = Bytes.toString(r.getValue(
					Bytes.toBytes("infos"), Bytes.toBytes("circonference")));

			System.out.println("Infos de l'arbre " + cle + " :");
			System.out.println("\tdate plantation:  " + datePlantation);
			System.out.println("\thauteur:  " + hauteur);
			System.out.println("\tcirconference:  " + circonference);

		} catch (IOException ex) {
			Logger.getLogger(Tp1.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public static void main(String[] args) {
		Tp1 tp1 = new Tp1();

		System.out.println();
		System.out.println("*** Les requêtes : ****");
		System.out.println();

		tp1.operation();
	}

}
