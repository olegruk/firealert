from faconfig import get_config
from faservice import str_to_lst

#dst_tabs = "['oopt_reg_points', 'oopt_reg_polygons', 'oopt_reg_clusters', 'oopt_reg_zones']"
#attr_fields = """['gid,id,status,category,adm_rf,name,actuality,cluster,location,designatio,year,source,scale', 'gid,id,status,category,region,name,actuality,area_ha,date,year,source,scale', 'gid,id,status,category,region,name,actuality,id_cluster,descriptio,name_clust,area_ha,date', 'gid,id,status,category,region,name,actuality,id_cluster,descriptio,name_clust,area_ha,date']"""

#def str_2_list(str):
#    res_list = str.split('\', \'')
#    res_list[0] = res_list[0][2:]
#    res_list[3] = res_list[3][:-2]
#   return res_list

#tab = str_2_list(dst_tabs)
#for i in range(len(tab)):
#    print(tab[i])

#attr = str_2_list(attr_fields)
#for i in range(len(attr)):
#    print(attr[i])

[dst_tabs, attr_fields] = get_config("aari", ["dst_tabs", "attr_fields"])
buf = get_config("clusters", "cluster_buf")
[link, period] = get_config("NASA", ["as_vnoaa_src_48h", "load_period"])
[log] = get_config("path", ["logfile"])
#[dbserver,dbport,dbname,dbuser,dbpass] = get_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])


for i in range(len(dst_tabs)):
    print(dst_tabs[i])
for i in range(len(attr_fields)):
    print(attr_fields[i])

print (buf)
print (link)
print(period)
print(log)
#print([dbserver,dbport,dbname,dbuser,dbpass])

#test = """['ab', "cd",  'ef',   'gh' ,'ij' , 'kl'  , 'mn']"""
#attr_fields = """["gid,id,status,category,adm_rf,name,actuality,cluster,location,designatio,year,source,scale",  "gid,id,status,category,region,name,actuality,area_ha,date,year,source,scale",        "gid,id,status,category,region,name,actuality,id_cluster,descriptio,name_clust,area_ha,date",        "gid,id,status,category,region,name,actuality,id_cluster,descriptio,name_clust,area_ha,date"]"""

#res = test.replace(r"\'\s*,\s*\'",",").split("\',\'")
#res = str_to_lst(attr_fields)
#print (resstr)
#res = resstr.split("\',\'")
#res[0] = res[0][2:]
#res[7] = res[7][:-2]
#for i in range(len(res)):
#    print(res[i])