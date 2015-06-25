package cice.hdfs.hadoopio;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;

public class CompactadorFicheros {
	private static FileSystem fs;

	/**
	 * @author Álvaro Sánchez Blasco
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		// Comprobamos que el programa recibe los argumentos necesarios
//		if (null != args && args.length == 2) {
			// Creamos la configuración del FileSystem
			Configuration conf = new Configuration();
			fs = FileSystem.get(new URI("hdfs://quickstart.cloudera:8020/"), conf);
			Path pathOrigen = new Path("/user/cloudera/data/*.seq");
//			Path pathOrigen = new Path(args[0]);
			Path ficheroDestino = new Path("/user/cloudera/datos.dat");
//			Path ficheroDestino = new Path(args[1]);
			// Comprobamos que el fichero destino NO existe
			if (fs.exists(ficheroDestino)) {
				System.err.println("\nEl fichero destino ya existe '" + ficheroDestino.getParent() + "/"
						+ ficheroDestino.getName() + "'\n");
			} else {
				// Recuperamos los datos del path origen
				FileStatus[] glob = fs.globStatus(pathOrigen);
				// Si tenemos datos...
				if (null != glob) {
					if (glob.length > 0) {
						// Creamos un writer, con compresión de tipo bloque, con
						// el codec BZip2
						// Como clave y valor, texto.
						SequenceFile.Writer writer = SequenceFile.createWriter(conf,
								Writer.file(fs.makeQualified(ficheroDestino)),
								SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class),
								SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new BZip2Codec()));
						System.out.println("\nProcesando ficheros\n");
						int i = 0;
						for (FileStatus fileStatus : glob) {
							if (i % 50 == 0) {
								System.out.println("Procesando ficheros\n");
							}
							i++;
							Path pFich = fileStatus.getPath();
							FSDataInputStream in = fs.open(pFich);
							// Leemos el contenido del fichero.
							byte[] content = new byte[(int) fs.getFileStatus(pFich).getLen()];
							in.readFully(content);
							// Cerramos el inputStream
							in.close();
							// Escribimos la linea nueva en el fichero
							if (writer != null) {
								writer.append(new Text(pFich.getName()), new Text(new String(content)));
							}
						}
						System.out.println("\nProcesado de datos finalizado\n");
						// Cerramos el Writer.
						writer.close();
					} else {
						System.err.println("\nEl directorio expresado no contiene datos '" + pathOrigen.getParent()
								+ "/" + pathOrigen.getName() + "'\n");
					}
					// Cerramos el objeto FileSystem.
					fs.close();
				} else {
					System.err.println("\nEl directorio expresado no existe '" + pathOrigen.getParent() + "/"
							+ pathOrigen.getName() + "'\n");
				}
			}
//		} else {
//			System.out.println("Usage: utad.hdfs.Compactador <path origen> <fichero destino>");
//		}
	}
}
