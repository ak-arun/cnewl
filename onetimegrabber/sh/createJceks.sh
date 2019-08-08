ddlStorePwd="****"
metaStorePwd="****"
jceksPwd="****"
jceksLocation=/home/hdfs/otg/conf/pwd.jceks
ddlPwdStoreKey=ddlstore.db.user.password
metaPwdKey=meta.db.user.password
java -cp /home/hdfs/otg/lib/onetimegrabber-0.0.1.jar com.ak.jceks.util.JCEKSBuilder $jceksLocation $jceksPwd $ddlPwdStoreKey $ddlStorePwd $metaPwdKey $metaStorePwd
