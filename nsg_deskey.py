import Lily.ctao2.ctao2_database_alias
from Lily.ctao2.ctao2_database_alias import manidb, alias, tickwatch
import des
from multiprocessing import Pool
class nsg_deskey:

    def __init__(self):
        import des

        self.deskey = des.DesKey(b'CTYANG_GINNY_LALUNA_ROSA')

    def encrypt(self, list):
        import binascii
        for word in list:
            #right pad a string with some character using string.rjust()
            #用_補足至長度16碼
            word = word.ljust(16, '_')
        
            #The way to convert string to bytes in Python 3!
            #將字串轉換成 二進位(binary) bytes
            word = str.encode(word, 'utf-8', errors='surrogatepass')

            #encrypt by des
            #des 加密
            word = self.deskey.encrypt(word)

            #將二進位轉換成16進位表達式
            word = binascii.hexlify(word)

            #將二進位轉換成utf-8文字
            word = word.decode('utf-8')

        return word

    def decrypt(self, word):
        import binascii
        
        #encrypt 的逆向工程
        word = self.deskey.decrypt(binascii.unhexlify(word)).decode('utf-8', errors='surrogatepass')
        return word.rstrip("_")

            
if __name__ == '__console__' or __name__ == '__main__':
    cputime         = tickwatch()
    #%%---------target-------------
    
    workdir                           = 'G:/NCREE_GIS/'
    db                                = manidb( workdir + 'htax/nsg_bldg_htax_raw_addr.sqlite' )
    output                            = manidb( workdir + 'htax/新北市地址資料整理＿嘉宏/nsg_bldg_raw_addr_newtaipei.sqlite' )
        
    cnty                              = db.get_alias('data_bnd_3826_county').read()
    
    for key, row in cnty[ cnty['ncity'] =='10017'].iterrows():
        mpool                         = Pool(8)
        target_tab                    = row['ncity']
        df_all                        = db.get_alias(f'rawdata_hou_A{target_tab}_addr').read()#.head(200)
                      
        #list1  = ['01012011333', '010119750521',  '010119990301']
        cputime.tick('read')
    #%%-------des_tool.encrypt()-------
        #df_all['enHOU_LOSN']         = [ nsg_deskey().encrypt(i) for i in df_all['HOU_LOSN'] ]
    
        df_all['enHOU_LOSN']         = mpool.map( nsg_deskey().encrypt, df_all['HOU_LOSN'].tolist() )
            
        select_cols                  = ['enHOU_LOSN', 'LOCAT_ADDR']
        df_all                       =  df_all[ select_cols ]
        output.get_alias(f'htax_addr_A{target_tab}').write(df_all)
        cputime.tick('htax address decomposition done')
    
    #%%-------des_tool.decrypt()-------
        #for en in enlist:
            #key = des_tool.decrypt(en)
            #print (key)