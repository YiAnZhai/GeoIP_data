# -*- coding: utf-8 -*-
import sys, os
from commands import getstatusoutput
import subprocess, time

import signal
from contextlib import contextmanager
@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise Exception("Timed out!")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


def exec_script( cmd, timeout ):
    print cmd
    check_time_screenshot = 5

    cmd_list = cmd.split(' ')
    # sub = subprocess.Popen(cmd_list, stdout = f_output, stderr = f_err, preexec_fn = os.setsid)
    f_output = ""
    sub = subprocess.Popen(cmd, preexec_fn = os.setsid, stdout = subprocess.PIPE, stderr = subprocess.PIPE,shell=True)
    time_start = time.time()
    i = 0
    while 1:
        ret1 = subprocess.Popen.poll(sub)
        if ret1 == 0:
            time_end = time.time()
            time_take = int(time_end - time_start + 0.5)
            return (0, sub.communicate()[0])
        elif ret1 is None:
            if i*check_time_screenshot >= timeout and timeout != -1:
                time_end = time.time()
                time_take = int(time_end - time_start + 0.5)

                sub.terminate()
                sub.wait()
                try:
                    os.killpg(sub.pid, signal.SIGTERM)
                except OSError as e:
                    pass

                #sub.kill()
                return (256, sub.communicate()[0])
            time.sleep(check_time_screenshot)
        else:
            time_end = time.time()
            time_take = int(time_end - time_start + 0.5)

            print sub.pid,'term', ret1
            return (1, sub.communicate()[0])
        i += 1
    return (1, sub.communicate()[0])



class HandleRemoteFile():
    def __init__( self, *arg_dict ):#, pem_path, remote_ip, remote_user, remote_path, remote_file, local_path ):
        self.tar_log_prefix = "cjdx_"
        self.success_flag = "cjdx_remote_success"
        self.ssh_no_check = " -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "

        self.pem_path = arg_dict[0].get( "pem_path" )
        self.remote_user = arg_dict[0].get( "remote_user" )
        self.remote_ip = arg_dict[0].get( "remote_ip" )
        self.remote_path = arg_dict[0].get( "remote_path" )
        self.remote_file = arg_dict[0].get( "remote_file" )
        self.local_path = arg_dict[0].get( "local_path" )

        self.remote_file_sitemap_start = arg_dict[0].get( "remote_file_sitemap_start" )
        self.sitemap_decompress_dir = arg_dict[0].get( "sitemap_decompress_dir" )
        self.compress_file_name = "%s%s.tar.gz" % ( self.tar_log_prefix, self.remote_file, )



    def compress_remote_log( self, t_out ):
        cmd_compress = "cd %s;sudo tar -cz -f '%s' '%s' && echo %s" % \
            (self.remote_path, self.compress_file_name, self.remote_file, self.success_flag)
        if self.pem_path:
            cmd_compress = """sudo ssh -i %s %s %s@%s "%s" """ % \
                (self.pem_path, self.ssh_no_check, self.remote_user, self.remote_ip, cmd_compress)

        # a = getstatusoutput( cmd_compress )
        a = exec_script( cmd_compress, t_out )
        return a


    def get_remote_log( self, t_out ):
        remote_file = os.path.join( self.remote_path, self.compress_file_name )
        if not os.path.exists( self.local_path ):
            os.makedirs( self.local_path )

        if self.pem_path:
            pem_arg = """ -i %s """ % (self.pem_path, )
            cmd_scp = """sudo scp %s %s %s@%s:%s %s && sudo ssh %s %s %s@%s "sudo rm '%s' && echo %s" """ % \
                ( pem_arg, self.ssh_no_check, self.remote_user, self.remote_ip, remote_file, self.local_path, \
                pem_arg, self.ssh_no_check, self.remote_user, self.remote_ip, remote_file, self.success_flag, )
        else:
            cmd_scp = """sudo cp %s %s && sudo rm '%s' && echo %s """ % \
                (remote_file, self.local_path, remote_file, self.success_flag)
        # a = getstatusoutput( cmd_scp )
        a = exec_script( cmd_scp, t_out )
        return a


    def log_archive( self ):
        cmd_decompress = """cd %s && sudo chmod +r %s && sudo tar -xz -f %s && sudo chmod +r %s && echo %s """ % \
            ( self.local_path, self.compress_file_name, self.compress_file_name, self.remote_file, self.success_flag )
        
        cmd_archive = """cd %s && sudo rm %s && echo %s""" % (self.local_path, self.compress_file_name, self.success_flag )
        re_decomp = getstatusoutput( cmd_decompress )
        if 0 == re_decomp[0] and self.success_flag in re_decomp[1]:print "log de_compress done"
        re_arch = getstatusoutput( cmd_archive )
        if 0 == re_arch[0] and self.success_flag in re_arch[1]:print "log archive done"


    def del_local_log_file( self ):
        local_file = os.path.join( self.local_path, self.remote_file )
        if not os.path.exists( local_file ):
            return False
        cmd_rm_log = "sudo rm %s" % (local_file, )
        re_log = getstatusoutput( cmd_rm_log )
        if 0 == re_log[0]:
            return True
        return False


    def main_get_log_file( self ):
        for i in range(2):
            re_com = self.compress_remote_log( 1200 )
            if 0 == re_com[0] and self.success_flag in re_com[1]:print "log file compress done";break
        else:
            return False
        for j in range(2):
            re_get = self.get_remote_log( 3600 )
            if 0 == re_get[0] and self.success_flag in re_get[1]:print "log file deliver done";break
        else:
            return False
        self.log_archive()
        return True


    #######sitemap
    def get_sitemap_file( self ):
        cmd_list_file = "ls %s && echo %s" % (self.remote_path, self.success_flag,)
        pem_arg = ""
        if self.pem_path:
            pem_arg = """ -i %s """ % (self.pem_path, )
            cmd_list_file = """sudo ssh %s %s %s@%s "%s" """ % (pem_arg, self.ssh_no_check, self.remote_user, self.remote_ip, cmd_list_file)
        re_list = getstatusoutput( cmd_list_file )
        # re_list = exec_script( cmd_list_file, 30 )
        if 0 == re_list[0] and self.success_flag in re_list[1]:
            files = [f for f in re_list[1].split("\n") if f.startswith(self.remote_file_sitemap_start)]
            if files:
                print files
                self.remote_sitemap_file = sorted(files, reverse=True)[0]
                remote_sitemap_full_path = os.path.join( self.remote_path, self.remote_sitemap_file )
                if self.pem_path:
                    cmd_scp = "sudo scp %s %s %s@%s:%s %s && echo %s" % ( pem_arg, self.ssh_no_check, self.remote_user, self.remote_ip, remote_sitemap_full_path, self.local_path, self.success_flag)
                else:
                    cmd_scp = "sudo scp %s %s && echo %s" % ( remote_sitemap_full_path, self.local_path, self.success_flag)
                re_scp = exec_script( cmd_scp, 1200 )
                if 0 == re_scp[0] and self.success_flag in re_scp[1]:
                    print "sitemap download done"
                    return True
        return False


    def decompress_sitemap( self ):
        local_file = os.path.join( self.local_path, self.remote_sitemap_file )
        cmd_archive = """cd %s && sudo chmod +r %s && sudo tar -xz -f %s && rm %s && echo %s""" % ( self.local_path, local_file, local_file, local_file, self.success_flag )
        flag = getstatusoutput( cmd_archive )
        if 0 == flag[0] and self.success_flag in flag[1]:
            print "sitemap decompress done"
            return True
        return False


    def del_local_sitemap_file( self ):
        sitemap_local_path = os.path.join( self.local_path, self.sitemap_decompress_dir )
        print sitemap_local_path
        files = []
        if os.path.exists( sitemap_local_path ):
            files = [f for f in os.listdir( sitemap_local_path ) if f.endswith(".xml")]
        if len(files) < 1:
            return False
        cmd_rm_sitemap = "sudo rm %s/*" % (sitemap_local_path, )
        re_sitemap = getstatusoutput( cmd_rm_sitemap )
        if 0 == re_sitemap[0]:
            return True
        return False

    def main_get_sitemap_file( self ):
        flag_get_file = self.get_sitemap_file()
        if flag_get_file:
            flag_decompress = self.decompress_sitemap()
            if flag_decompress:
                return True
            else:
                return False
        else:
            return False


if "__main__" == __name__:
    # arg_dict = {
    #     "pem_path":"/home/moma/Documents/security/all_pem/new_pem/couponsKey.pem",
    #     "remote_ip":"54.85.235.215",
    #     "remote_user":"ubuntu",
    #     "remote_path":"/usr/local/nginx/logs",
    #     "remote_file":"access.log.1",
    #     "local_path":"/home/moma/Documents/project/Log_Visualization/log_file/couponbirds",
    # }
    arg_dict = {
        "pem_path":"./pem_keys/couponsKey.pem",
        "remote_ip":"54.85.235.215",
        "remote_user":"ubuntu",
        "remote_path":"/home/ubuntu/zhaoxueqin24",
        "remote_file_sitemap_start":"sitemap",
        "sitemap_decompress_dir":"sitemap",
        "remote_index_file":"sitemap98349022_couponsite2016index.xml",
        "local_path":"/home/moma/Documents/project/Log_Visualization/log_file/couponbirds",
    }
    remote_handle = HandleRemoteFile( arg_dict )
    # remote_handle = HandleRemoteFile( pem_path, remote_ip, remote_user, remote_path, remote_file, local_path )
    remote_handle.get_sitemap_file( )
    remote_handle.decompress( )
    # remote_handle.del_local_sitemap_file( )