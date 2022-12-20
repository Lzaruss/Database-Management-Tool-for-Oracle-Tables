import java.sql.*;
import java.util.Scanner;

public class CreacionyMantenimientoTablasOracle {

    Scanner reader = new Scanner(System.in);

    boolean authentication = true;
    String tableOption2 = null;

    void menu(Connection conexion) {
        int respuesta = 0;
        System.out.println("1.- Consulta de la estructura de una tabla\n"
                + "2.- Creación de un tabla\n"
                + "3.- Mantenimiento de datos de una tabla\n"
                + "4.- Consulta de la totalidad de los datos de cualquier tabla\n"
                + "5.- Salir\n");

        try {
            respuesta = reader.nextInt();
        } catch (java.util.InputMismatchException e) {
            System.out.println("No has introducido una opción valida...\nSaliendo...\n");
            System.exit(0);
        }

        switch (respuesta) {
            case 1:
                firstOption(conexion);
            case 2:
                secondOption(conexion);
            case 3:
                thirdOption(conexion);
            case 4:
                fourthOption(conexion);
            case 5:
                System.exit(0);
            default:
                menu(conexion);
        }
    }

    public static void main(String[] args) {
        CreacionyMantenimientoTablasOracle app = new CreacionyMantenimientoTablasOracle();

        Scanner reader = new Scanner(System.in);
        System.out.println("Introduce el usuario:");
        String user = reader.next();
        System.out.println("Introduce la contraseña del usuario:");
        String psw = reader.next();
        
        try {
            Class.forName("oracle.jdbc.OracleDriver");

            Connection conexion = DriverManager.getConnection("jdbc:oracle:thin:"+user+"/"+psw+"@//localhost:1521/XEPDB1");

            app.menu(conexion);

            //Aqui ya cerramos la conexion por completo
            conexion.close();
        } catch (ClassNotFoundException cn) {
            cn.printStackTrace();
        } catch (SQLException ex) {
            System.out.println("El usuario y/ó la contraseña son incorrectos");
            System.exit(0);
        }
    }

    private void firstOption(Connection conexion) {
        System.out.println("\nTe has metido en la opcion 1\n");
        try {
            Statement sentencia = conexion.createStatement();
            int encontrado = 0;
            do {
                //RECUPERANDO DATOS DE LA TABLA
                System.out.println("Introduce el nombre de una tabla:");
                String tabla = reader.next();
                String sql = "SELECT * FROM user_tables WHERE table_name = '" + tabla + "'";
                ResultSet st = sentencia.executeQuery(sql);

                if (st.next()) {
                    encontrado = 1;
                    String sql2 = "Select * from " + tabla;
                    ResultSet filas;
                    Statement sentencia2 = conexion.createStatement();
                    System.out.println("Estructura de la tabla " + tabla + ":");
                    filas = sentencia2.executeQuery(sql2);
                    ResultSetMetaData MDFilas = filas.getMetaData();
                    int n = MDFilas.getColumnCount();
                    for (int i = 1; i <= n; i++) {
                        System.out.println("\nNombre de la columna: " + MDFilas.getColumnName(i)
                                + "\nTamaño: " + MDFilas.getColumnDisplaySize(i)
                                + "\nTipo: " + MDFilas.getColumnTypeName(i) + "\n");
                    }

                    //RECUPERANDO CLAVES PRIMARIAS
                    ResultSet rs = null;
                    DatabaseMetaData meta = conexion.getMetaData();
                    rs = meta.getPrimaryKeys(null, null, tabla);
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        System.out.println("Primary Keys: Nombre de la columna = " + columnName);
                    }

                    System.out.println("\n");
                    //RECUPERANDO EXPORTED KEYS
                    rs = meta.getExportedKeys(conexion.getCatalog(), null, tabla);
                    while (rs.next()) {
                        String fkTableName = rs.getString("FKTABLE_NAME");
                        System.out.println("Foreign Keys: Nombre de la tabla = " + fkTableName);
                    }

                    System.out.println("\n");
                    //RECUPERANDO IMPORTED KEYS
                    rs = meta.getImportedKeys(conexion.getCatalog(), null, tabla);
                    while (rs.next()) {
                        String fkTableName = rs.getString("FKTABLE_NAME");
                        String fkColumnName = rs.getString("FKCOLUMN_NAME");
                        System.out.println("Imported Keys: Nombre de la tabla = " + fkTableName);
                        System.out.println("Imported Keys: Nombre de la columna = " + fkColumnName);

                    }

                    filas.close();
                    sentencia2.close();
                    volverMenu(conexion);
                        
                } else {
                    System.out.println("La tabla no existe");
                    volverMenu(conexion);
                }
            } while (encontrado == 0);

        } catch (SQLException ex) {
            errorSQL();

        }
    }

    private void secondOption(Connection conn) {
        System.out.println("\nTe has metido en la opcion 2\n");
        System.out.println("Introduce el nombre de una tabla:");
        String tabla = reader.next();
        String query = "select * from  " + tabla + " where 1=0";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            System.out.println("Esa tabla ya existe...\nVolviendo al menu..\n");
            menu(conn);
        } catch (Exception e) {
            //Se meterá en este Catch cuando la tabla en concreto exista
            try {
                //La tabla tratará de hospitales
                String createString = "create table " + tabla
                        + "(HOSP_ID INTEGER NOT NULL, "
                        + "HOSP_NAME VARCHAR(40), "
                        + "STREET VARCHAR(40), "
                        + "CITY VARCHAR(20), "
                        + "STATE VARCHAR(20), "
                        + "primary key(HOSP_ID))";
                stmt.executeUpdate(createString);
                DatabaseMetaData dbmd = conn.getMetaData();
                rs = dbmd.getPrimaryKeys(null, null, tabla);
                while (rs.next()) {
                    String name = rs.getString("TABLE_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    System.out.println("table name :  " + name);
                    System.out.println("column name:  " + columnName);
                    System.out.println("");
                }
                this.tableOption2 = tabla;
                rs.close();
                volverMenu(conn);
                        
            } catch (SQLException ex) {
                errorSQL();
            }
        }
    }

    private void thirdOption(Connection conn) {

        if (this.tableOption2 == null) {
            System.out.println("Esta opcion solo se contemplará cuando se cree una tabla en la opcion 2...\nVolviendo al menu...\n");
            menu(conn);
        }
        System.out.println("\nTe has metido en la opcion 3\nEstas en la tabla: " + this.tableOption2);
        String menuOptions = "1.- Anadir una fila a la tabla.\n"
                + "2.- Visualizar los datos de una fila de la tabla.\n"
                + "3.- Visualizar todas las filas que cumplan una condición de una columna de la tabla (distinta al código).\n"
                + "4.- Eliminar una fila de la tabla.\n"
                + "5.- Volver al menu\n";
        System.out.println(menuOptions);
        int respuesta = 0;
        try {
            respuesta = reader.nextInt();
        } catch (java.util.InputMismatchException e) {
            System.out.println("No has introducido una opción valida...\nSaliendo...\n");
            System.exit(0);
        }
        switch (respuesta) {
            case 1:
                try {
                    Statement sentencia = conn.createStatement();
                    System.out.println("Codigo del hospital: ");
                    String hosp_id = reader.next();
                    System.out.println("Nombre del hospital: ");
                    String hosp_name = reader.next();
                    System.out.println("Nombre de la calle: ");
                    String street_name = reader.next();
                    System.out.println("Nombre de la ciudad: ");
                    String city_name = reader.next();
                    System.out.println("Nombre del pais: ");
                    String state_name = reader.next();
                    String sql = "INSERT INTO " + this.tableOption2 + " (HOSP_ID,HOSP_NAME,STREET,CITY,STATE) "
                            + "VALUES ('" + hosp_id + "','" + hosp_name + "','" + street_name + "','" + city_name + "','" + state_name + "')";
                    int nreg;
                    nreg = sentencia.executeUpdate(sql);
                    sentencia.close();
                    volverMenu(conn);
            } catch (SQLException ex) {
                errorSQL();
            }
            ;
            case 2:
                try {
                    System.out.println("Introduce el codigo del hospital: ");
                    String hosp_id = reader.next();

                    String sql = "SELECT HOSP_ID,HOSP_NAME,STREET,CITY,STATE"
                            + " FROM " + this.tableOption2
                            + " WHERE HOSP_ID = " + hosp_id;
                    Statement sent = conn.createStatement();
                    ResultSet filas;
                    System.out.println("\n\nDatos del hospital insertado");
                    filas = sent.executeQuery(sql);
                    while (filas.next()) {
                        System.out.println("ID:       " + filas.getString(1));
                        System.out.println("Nombre:   " + filas.getString(2));
                        System.out.println("Calle:    " + filas.getString(3));
                        System.out.println("Ciudad:   " + filas.getString(4));
                        System.out.println("Pais:     " + filas.getString(5));
                    }
                    filas.close();
                    sent.close();                        
                    volverMenu(conn);
            } catch (SQLException ex) {        
                errorSQL();
            }
            ;
            case 3:

                System.out.println("Elige de que columna harás la condición\n"
                        + "1.- NOMBRE\n2.- CALLE\n3.- CIUDAD\n4.- PAIS");
                int option = reader.nextInt();
                switch (option) {
                    case 1:
                        System.out.println("Escribe el nombre: ");
                        String name = reader.next();
                        String sql = "SELECT HOSP_ID,HOSP_NAME,STREET,CITY,STATE"
                                + " FROM " + this.tableOption2
                                + " WHERE HOSP_NAME = '" + name + "'";
                        condicionesSQL(conn, sql);
                        ;
                    case 2:
                        System.out.println("Escribe la calle: ");
                        String calle = reader.next();
                        sql = "SELECT HOSP_ID,HOSP_NAME,STREET,CITY,STATE"
                                + " FROM " + this.tableOption2
                                + " WHERE STREET = '" + calle + "'";
                        condicionesSQL(conn, sql);
                        ;
                    case 3:
                        System.out.println("Escribe la ciudad: ");
                        String ciudad = reader.next();
                        sql = "SELECT HOSP_ID,HOSP_NAME,STREET,CITY,STATE"
                                + " FROM " + this.tableOption2
                                + " WHERE CITY = '" + ciudad + "'";
                        condicionesSQL(conn, sql);
                        ;
                    case 4:
                        System.out.println("Escribe el pais: ");
                        String pais = reader.next();
                        sql = "SELECT HOSP_ID,HOSP_NAME,STREET,CITY,STATE"
                                + " FROM " + this.tableOption2
                                + " WHERE STATE = '" + pais + "'";
                        condicionesSQL(conn, sql);
                        ;
                }
                ;
            case 4:
                try {
                    Statement sentencia2 = conn.createStatement();
                    System.out.println("Introduzca el codigo del hospital");
                    String hosp_id = reader.next();

                    String sql = "SELECT HOSP_ID,HOSP_NAME,STREET,CITY,STATE"
                                    + " FROM " + this.tableOption2
                                    + " WHERE HOSP_ID = " + hosp_id;
                    Statement sentencia = conn.createStatement();
                    ResultSet filas = sentencia.executeQuery(sql);
                    if (filas.next()) {
                        System.out.println("\nDatos del hospital:");
                        System.out.println("ID:       " + filas.getString(1));
                        System.out.println("Nombre:   " + filas.getString(2));
                        System.out.println("Calle:    " + filas.getString(3));
                        System.out.println("Ciudad:   " + filas.getString(4));
                        System.out.println("Pais:     " + filas.getString(5));

                        System.out.println("\n\nQuiere borrar los datos de este hospital(S/N)");
                        String resp = reader.next();
                        if (resp.equals("S")) {
                            String sql2 = "delete from "+this.tableOption2+" where hosp_id = " + hosp_id;
                            int nreg;
                            /* nreg será el número de filas que han sido actualizadas*/
                            nreg = sentencia2.executeUpdate(sql2);
                            System.out.println("El hospital: " + hosp_id + " ha sido eliminado de la base de datos");
                            sentencia2.close();
                        }

                    } else {
                        System.out.println("Hospital no existe");
                    }
                    filas.close();
                    sentencia.close();
                    volverMenu(conn);
            }catch (SQLException e) {        
                errorSQL();
            }
            ;
        case 5:
            menu(conn);
        }
    }

    void condicionesSQL(Connection conn, String sql) {

        try {
            Statement sent = conn.createStatement();
            ResultSet filas;
            System.out.println("\n\nDatos del hospital");
            filas = sent.executeQuery(sql);
            if (filas.next()) {
                while (filas.next()) {
                    System.out.println("ID:       " + filas.getString(1));
                    System.out.println("Nombre:   " + filas.getString(2));
                    System.out.println("Calle:    " + filas.getString(3));
                    System.out.println("Ciudad:   " + filas.getString(4));
                    System.out.println("Pais:     " + filas.getString(5));
                }
            } else {
                System.out.println("No se ha encontrado el hospital");
            }
            filas.close();
            sent.close();
            volverMenu(conn);
        } catch (SQLException ex) {
            errorSQL();
        }
    }
    
    private void fourthOption(Connection conexion) {
        try {
            System.out.println("\nTe has metido en la opcion 4\n");
            String Tabla;
            int encontrado = 0;
            Statement sentencia = conexion.createStatement();
            System.out.println("Introduzca el nombre de la tabla:");
            Tabla = reader.next();
            do {

                String sql = "Select * from user_tables where table_name = '" + Tabla + "'";
                ResultSet filas = sentencia.executeQuery(sql);
                if (filas.next()) {

                    encontrado = 1;
                    String sql2 = "Select * from " + Tabla;
                    ResultSet filas2;
                    Statement sentencia2 = conexion.createStatement();
                    System.out.println("Datos de la tabla " + Tabla + "\n");
                    
                    filas2 = sentencia2.executeQuery(sql2 + "\n");
                    ResultSetMetaData MDfilas = filas2.getMetaData();
                    int n = MDfilas.getColumnCount();
                    int registros = 0;
                    for (int i = 1; i <= n; i++) {
                        System.out.print(MDfilas.getColumnName(i) + "    ");
                    }
                    System.out.println("\n---------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                    while (filas2.next()) {
                        for (int i = 1; i <= n; i++) {
                            if (i != n) {
                                System.out.print(filas2.getString(i) + "  --  ");
                            } else {
                                System.out.print(filas2.getString(i));
                            }
                        }
                        System.out.println("");
                        registros += 1;
                    }
                    System.out.println("\nHay " + registros + " filas en la tabla " + Tabla);
                    filas2.close();
                    sentencia2.close();

                } else {
                    System.out.println("La tabla no existe");
                    menu(conexion);

                }
            } while (encontrado == 0);
            volverMenu(conexion);
            
        } catch (SQLException ex) {
            errorSQL();
        }
    }
    
    //Metodos auxiliares para no tener que estar escribiendo lo mismo constantemente
    void volverMenu(Connection conexion){
        System.out.println("\nRegresando al menu...\n");
        menu(conexion);       
    }
    
    void errorSQL(){
        System.out.println("Error de SQL...\nSALIENDO...");
        System.exit(0);
    }
}