package cice.hdfs.hadoopio;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializadorFicheros {
	
	private static final Logger logger = LoggerFactory.getLogger(SerializadorFicheros.class);
	
	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://quickstart.cloudera:8020/"), conf);
		
		Path fichero = new Path("data/quijote.txt");
		
		FSDataInputStream in = hdfs.open(fichero);
		/*
		 * A partir de este punto podríamos leerlo como haríamos con un fichero
		 * cualquiera en el filesystem local, con un Reader, por ejemplo, para
		 * leerlo línea a línea. Algo como: BufferedReader br = new
		 * BufferedReader(new InputStreamReader(in)); while ((String linea =
		 * br.readLine()) != null) { System.out.println(linea); // No lo
		 * recomiendo... } Si fuésemos a crear el fichero secuencial de forma
		 * que en cada registro tuviéramos como valor una línea del libro y como
		 * clave, por ejemplo, el número de línea, éste podría ser un mecanismo
		 * para hacerlo.
		 */
		/*
		 * Sin embargo, en este ejemplo lo que vamos a crear es un único
		 * registro en el fichero de salida, con todo el texto del libro (la
		 * clave será algo arbitrario), así que vamos a ver una forma mejor de
		 * leer el contenido del fichero completo.
		 */
		// Leer el fichero completo utilizando el método readFully.
		// 1. Obtener el FileStatus del fichero a través del objeto FileSystem.
		// 2. Obtener el tamaño del fichero a través de su FileStatus.
		// 3. Crear un byte[] de ese tamaño.
		// 4. Invocar a readFully() pasándole el array anterior.
		byte[] content = new byte[(int) hdfs.getFileStatus(fichero).getLen()];
		in.readFully(content);
		
		// Cerrar el stream de lectura.
		in.close();
		
		// Llegados a este punto tenemos el fichero “data/quijote.txt” volcado
		// sobre la variable content. Ahora hay que crear un fichero secuencial
		// y
		// escribir en él este contenido.
		// Crear un objeto SequenceFile.Writer para escribir sobre un fichero
		// llamado “data/quijote.seq”.
		// Antes habríamos hecho:
		// // Ver api
		// SequenceFile.Writer outSeq = SequenceFile.createWriter(
		// hdfs,
		// conf,
		// new Path("data/quijote.seq"),
		// IntWritable.class,
		// Text.class);
		// Actualmente lo haremos así:
		// ...
		/**
		 * @see http://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/io/
		 *      SequenceFile.html
		 */
//		SequenceFile.Writer outSeq = SequenceFile.createWriter(conf,
//				SequenceFile.Writer.file(hdfs.makeQualified(new Path("data/quijote.seq"))),
//				SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class));
//		
		// Se ha incluido una llamada a FileSystem#makeQualified() ya que no
		// tenemos cargada la configuración en el objeto conf, y si no hacemos
		// que
		// la ruta esté referida al FileSystem con el que queremos trabajar, por
		// defecto, creará el fichero en local. Otra opción habría sido escribir
		// la ruta
		// completa
		// (“hdfs://quickstart.cloudera:8020/user/cloudera/data/quijote.seq”), y
		// la
		// otra (más recomendable) haber hecho una Configured Tool, pero esa
		// parte se verá en el siguiente módulo.
		// A continuación, escribimos en el fichero secuencial el contenido
		// leído en
		// los primeros pasos.

		for (int i=0;i<20;i++) {
			SequenceFile.Writer outSeq = SequenceFile.createWriter(conf,
					SequenceFile.Writer.file(hdfs.makeQualified(new Path("data/quijote"+i+".seq"))),
					SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class));

			outSeq.append(new IntWritable(i), new Text(new String(content)));
			// 3. Después cerramos el Writer.
			outSeq.close();			
		}


		
		// Y, por último, cerramos el objeto FileSystem.
		hdfs.close();
		
		getLogger().info("Fin del proceso.\n hdfs dfs -text data/quijote.seq | head");
		// hdfs dfs -text data/quijote.seq | head
	}

	/**
	 * @return the logger
	 */
	public static final Logger getLogger() {
		return logger;
	}
}
