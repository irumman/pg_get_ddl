CREATE OR REPLACE FUNCTION get_ddl(object_type varchar(10),object_name varchar(30))
RETURNS text
AS $$
  import os
  v_object_type =  object_type
  v_object_name = object_name
  v_dbname = 'postgres'
  v_schema = 'public'
  v_port = '5432'
  v_debug = 1
  
  global v_object_name
  global v_schema
  
  def debug_log(cmd):
    if v_debug == 1: 
      cmd = cmd.replace("'","''")
      rv = plpy.execute("INSERT INTO log VALUES ('" + cmd + "')")
  
  def get_current_database():
    rv = plpy.execute("SELECT CURRENT_DATABASE() as dbname")
    dbname = rv[0]["dbname"]
    return dbname
  #def get_current_database():
  
  def get_connection_port():
    rv = plpy.execute("SHOW port")
    port = rv[0]["port"]
    return port
  
  def get_schema_name(object_name):
    global v_schema
    global v_object_name
    v_str =  str.split(object_name,'.')
    if len(v_str) == 1:
      v_schema = 'public'
      v_object_name = v_str[0]
      debug_log('Found user given object name with no dot(.): v_object_name = ' +  v_object_name + " and v_schema  = " + v_schema)
      return 1
    elif len(v_str) == 2:
      v_schema = v_str[0]
      v_object_name = v_str[1]
      debug_log('Found user given object name with two dots(.): v_object_name = ' +  v_object_name + " and v_schema  = " + v_schema)
      return 1
    else:
      debug_log('Found user given object name with more than two dots(.)')
      return 0    
    #def get_schema_name():  
  
  def run_command():
    o = ''
    
    cmd = 'pg_dump ' + v_dbname + ' -p ' + v_port + ' -s  -' + v_object_type + ' '+ object_name
    debug_log(cmd)
    
    stream = os.popen(cmd)
    for line in stream.readlines():
       if line == '\n' or line == '--\n' or ('PostgreSQL database dump' in line) or  ('SET' in line):
         continue
       o = o + line.replace(';\n',';\n\n')
    return  o
  #end of def run_command():
  
  def check_table_exists(v_object_name,v_schema):
    cmd = "SELECT relname as tablename FROM pg_stat_user_tables WHERE relname = '" + v_object_name + "' AND schemaname = '" + v_schema + "'" 
    debug_log(cmd)
    rv = plpy.execute(cmd)
    
    if len(rv) > 0 :
      return 1
    else:
      return 0
  #end of def check_table_exists(): 
  
  # main()
  
  if v_object_type.lower() in ( 'table' ,'sequence','view'):
    v_object_type = 't'
    debug_log('Object is table and entered into the block')
    if get_schema_name(v_object_name) == 0:
      return 'Object name cannot be like "' + object_name
    debug_log('After get_schema_name: v_object_name = ' + v_object_name + " and  v_schema = " + v_schema)  
    
    if v_object_type.lower() == 'table': 
      if check_table_exists(v_object_name,v_schema) == 0 :
        return 'Table "' + object_name + '" does not exist in the database'
      
      
    
  v_dbname = get_current_database()  
  global v_port
  v_port = get_connection_port()
  output = run_command()
  return output
$$ Language 'plpythonu';
