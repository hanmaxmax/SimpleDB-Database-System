package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    /**
     * 根据下面给出的成员函数，确定类的成员对象需要有：
     * 1. HashMap from id(int) to file(DbFile)
     * 2. HashMap from id(int) to name(String)
     * 3. HashMap from id(int) to pkeyField(String)
     * 4. HashMap from name(String) to id(int)
     */
    private HashMap<Integer,DbFile> id2file;
    private HashMap<Integer,String> id2name;
    private HashMap<Integer, String> id2pkeyField;
    private HashMap<String, Integer> name2id;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        /**
         * new一下成员属性
         */
        id2file = new HashMap<>();
        id2pkeyField = new HashMap<>();
        id2name = new HashMap<>();
        name2id = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        //这句代码先这么写，以后有问题再改————————————————————————————————————————————————————————————————————————————————
        if (name == null || pkeyField == null || file == null) {
            throw new IllegalArgumentException();
        }

        int fileId=file.getId();
        //如果已经有重复的name时，先删除之前存在的表，再添加新表
        //有name2id和id2name说明id和name都不能重复
        if(name2id.containsKey(name)){
            int nowId=name2id.get(name);//找到现在已有的id，然后把它删掉
            id2file.remove(nowId);
            id2pkeyField.remove(nowId);
            id2name.remove(nowId);
            name2id.remove(name);
        }
        //添加新表
        id2file.put(fileId, file);
        id2name.put(fileId, name);
        id2pkeyField.put(fileId, pkeyField);
        name2id.put(name, fileId);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null || !name2id.containsKey(name))
            throw new NoSuchElementException("name is null or name doesn't exist.");
        return name2id.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if(! id2file.containsKey(tableid))
            throw new NoSuchElementException("tableid doesn't exist");
        return id2file.get(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if(! id2file.containsKey(tableid))
            throw new NoSuchElementException("tableid doesn't exist");
        return id2file.get(tableid);
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if(! id2pkeyField.containsKey(tableid))
            throw new NoSuchElementException("tableid doesn't exist");
        return id2pkeyField.get(tableid);

    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return id2name.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        if(! id2name.containsKey(id))
            throw new NoSuchElementException("id doesn't exist");
        return id2name.get(id);
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        id2name.clear();
        id2pkeyField.clear();
        id2file.clear();
        name2id.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

