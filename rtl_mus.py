#!/usr/bin/python2
'''
This file is part of RTL Multi-User Server,
	that makes multi-user access to your DVB-T dongle used as an SDR.
Copyright (c) 2013 by Andras Retzler <retzlerandras@gmail.com>

RTL Multi-User Server is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

RTL Multi-User Server is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with RTL Multi-User Server.  If not, see <http://www.gnu.org/licenses/>.

-----

2013-11?  Asyncore version
#2014-03   Fill with null on no data

'''

from __future__ import print_function, unicode_literals
import socket
import sys
import array
import time
import logging
import os
import time
import subprocess
try:
    import thread
except ImportError:
    import _thread as thread
import asyncore
import multiprocessing

import traceback

LOGGER = logging.getLogger("rtl_mus")


def ip_match(this, ip_ranges):
    return not ip_ranges or any(this.startswith(ip_range) for ip_range in ip_ranges)


def ip_access_control(ip):
    if not CONFIG.use_ip_access_control:
        return True
    allowed = ip_match(ip, CONFIG.allowed_ip_ranges)
    denied = ip_match(ip, CONFIG.denied_ip_ranges)
    if CONFIG.order_allow_deny:
        return False if denied else allowed
    else:
        return True if allowed else not denied


def dsp_read_thread():
    global dsp_data_count
    while True:
        try:
            my_buffer = proc.stdout.read(1024)
        except IOError:
            LOGGER.error("DSP subprocess is not ready for reading.")
            time.sleep(1)
            continue
        SERVER.add_data_to_clients(my_buffer)
        if CONFIG.debug_dsp_command:
            dsp_data_count += len(my_buffer)


def dsp_write_thread():
    global original_data_count
    while True:
        try:
            my_buffer = dsp_input_queue.get(timeout=0.3)
        except Exception:
            continue
        proc.stdin.write(my_buffer)
        proc.stdin.flush()
        if CONFIG.debug_dsp_command:
            original_data_count += len(my_buffer)


class Client(asyncore.dispatcher):

    def __init__(self, socket, addr, port, identifier):
        self.ident = identifier
        self.waiting_data = multiprocessing.Queue(250)
        self.start_time = time.time()
        self.address = addr
        self.port = port
        self.sent_dongle_id = False
        self.last_waiting_buffer = b""
        asyncore.dispatcher.__init__(self, socket)

    def handle_read(self):
        command = bytearray(self.recv(5))
        if len(command) >= 5:
            if self.command_allowed(command):
                commands.put(command)

    def command_allowed(self, command):
        global sample_rate
        param = array.array("I", command[1:5])[0]
        param = socket.ntohl(param)
        command_id = command[0]
        if time.time() - self.start_time < CONFIG.client_cant_set_until and not (CONFIG.first_client_can_set and self.ident == 0):
            LOGGER.info("deny: %s -> client can't set anything until %d seconds", self, CONFIG.client_cant_set_until)
            return 0
        if command_id == 1:
            if max(map((lambda r: param >= r[0] and param <= r[1]), CONFIG.freq_allowed_ranges)):
                LOGGER.debug("allow: %s -> set freq %s", self, param)
                return 1
            else:
                LOGGER.debug("deny: %s -> set freq - out of range: %s", self, param)
        elif command_id == 2:
            LOGGER.debug("deny: %s -> set sample rate: %s", self, param)
            sample_rate = param
            return 0  # ordinary clients are not allowed to do this
        elif command_id == 3:
            LOGGER.debug("deny/allow: %s -> set gain mode: %s", self, param)
            return CONFIG.allow_gain_set
        elif command_id == 4:
            LOGGER.debug("deny/allow: %s -> set gain: %s", self, param)
            return CONFIG.allow_gain_set
        elif command_id == 5:
            LOGGER.debug("deny: %s -> set freq correction: %s", self, param)
            return 0
        elif command_id == 6:
            LOGGER.debug("deny/allow: %s -> set if stage gain", self)
            return CONFIG.allow_gain_set
        elif command_id == 7:
            LOGGER.debug("deny: %s -> set test mode", self)
            return 0
        elif command_id == 8:
            LOGGER.debug("deny/allow: %s -> set agc mode", self)
            return CONFIG.allow_gain_set
        elif command_id == 9:
            LOGGER.debug("deny: %s -> set direct sampling", self)
            return 0
        elif command_id == 10:
            LOGGER.debug("deny: %s -> set offset tuning", self)
            return 0
        elif command_id == 11:
            LOGGER.debug("deny: %s -> set rtl xtal", self)
            return 0
        elif command_id == 12:
            LOGGER.debug("deny: %s -> set tuner xtal", self)
            return 0
        elif command_id == 13:
            LOGGER.debug("deny/allow: %s -> set tuner gain by index", self)
            return CONFIG.allow_gain_set
        else:
            LOGGER.debug("deny: %s sent an ivalid command: %s", self, param)
        return 0

    def handle_error(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        LOGGER.info("client error: %s", self)
        LOGGER.exception(exc_value)
        self.close()

    def handle_close(self):
        self.close()
        LOGGER.info("client disconnected: %s", self)

    def writable(self):
        # print("queryWritable",not self.waiting_data.empty())
        return not self.waiting_data.empty()

    def handle_write(self):
        if not self.sent_dongle_id:
            self.send(rtl_dongle_identifier)
            self.sent_dongle_id = True
            return
        # print("write2client",self.waiting_data.qsize())
        next = self.last_waiting_buffer + self.waiting_data.get()
        sent = asyncore.dispatcher.send(self, next)
        self.last_waiting_buffer = next[sent:]

    def close(self):
        SERVER.remove_client(self)
        asyncore.dispatcher.close(self)

    def add_data(self, data):
        # print("self %d size: %d"%(self.ident,self.waiting_data.qsize()))
        if self.waiting_data:
            if self.waiting_data.full():
                if CONFIG.cache_full_behaviour == 0:
                    LOGGER.error("client cache full, dropping samples: %s", self)
                    while not self.waiting_data.empty():  # clear queue
                        self.waiting_data.get(False, None)
                elif CONFIG.cache_full_behaviour == 1:
                    # rather closing client:
                    LOGGER.error("client cache full, dropping client: %s", self)
                    self.close()
                elif CONFIG.cache_full_behaviour == 2:
                    pass  # client cache full, just not taking care
                else:
                    LOGGER.error("invalid value for CONFIG.cache_full_behaviour")
            else:
                self.waiting_data.put(data)

    def __str__(self):
        return '{}@{}:{}'.format(self.ident, self.address, self.port)


class ServerAsyncore(asyncore.dispatcher):

    def __init__(self):
        self.clients = set()
        self.clients_mutex = multiprocessing.Lock()
        self.client_count = 0
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((CONFIG.my_ip, CONFIG.my_listening_port))
        self.listen(5)
        LOGGER.info("Server listening on port: %s", CONFIG.my_listening_port)

    def handle_accept(self):
        accept = self.accept()
        if accept is None:  # not sure if required
            return
        socket, (addr, port) = accept
        if ip_access_control(addr):
            client = Client(socket, addr, port, self.client_count)
            self.client_count += 1
            self.clients_mutex.acquire()
            self.clients.add(client)
            LOGGER.info("client accepted: %s  users now: %d", client, len(self.clients))
            self.clients_mutex.release()
        else:
            LOGGER.info("client denied: %s blocked by ip", addr)
            socket.close()

    def add_data_to_clients(self, data):
        # might be called from:
        # -> dsp_read
        # -> rtl_tcp_asyncore.handle_read
        # -> watchdog filling missing data
        self.clients_mutex.acquire()
        for client in self.clients:
            client.add_data(data)
        self.clients_mutex.release()

    def remove_client(self, client):
        self.clients_mutex.acquire()
        self.clients.remove(client)
        self.clients_mutex.release()


rtl_tcp_resetting = False  # put me away


def rtl_tcp_asyncore_reset(timeout):
    global RTL_TCP
    global rtl_tcp_resetting
    if rtl_tcp_resetting:
        return
    # print("rtl_tcp_asyncore_reset")
    rtl_tcp_resetting = True
    time.sleep(timeout)
    try:
        RTL_TCP.close()
    except Exception:
        pass
    try:
        del RTL_TCP
    except Exception:
        pass
    RTL_TCP = RtlTcpAsyncore()
    # print(asyncore.socket_map)
    rtl_tcp_resetting = False


class RtlTcpAsyncore(asyncore.dispatcher):
    def __init__(self):
        asyncore.dispatcher.__init__(self)
        self.ok = True
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.connect((CONFIG.rtl_tcp_host, CONFIG.rtl_tcp_port))
            self.socket.settimeout(0.1)
        except Exception:
            LOGGER.error("rtl_tcp connection refused. Retrying.")
            thread.start_new_thread(rtl_tcp_asyncore_reset, (1,))
            self.close()
            return

    def handle_error(self):
        global server_missing_logged
        global rtl_tcp_connected
        rtl_tcp_connected = False
        exc_type, exc_value, exc_traceback = sys.exc_info()
        self.ok = False
        server_is_missing = hasattr(exc_value, "errno") and exc_value.errno == 111
        if (not server_is_missing) or (not server_missing_logged):
            LOGGER.exception(exc_value)
            server_missing_logged |= server_is_missing
        try:
            self.close()
        except Exception:
            pass
        thread.start_new_thread(rtl_tcp_asyncore_reset, (2,))

    def handle_connect(self):
        global server_missing_logged
        global rtl_tcp_connected
        self.socket.settimeout(0.1)
        rtl_tcp_connected = True
        if self.ok:
            LOGGER.info("rtl_tcp host connection estabilished")
            server_missing_logged = False

    def handle_close(self):
        global rtl_tcp_connected
        rtl_tcp_connected = False
        LOGGER.error("rtl_tcp host connection has closed, now trying to reopen")
        try:
            self.close()
        except Exception:
            pass
        thread.start_new_thread(rtl_tcp_asyncore_reset, (2,))

    def handle_read(self):
        global rtl_dongle_identifier
        global watchdog_data_count
        if len(rtl_dongle_identifier) == 0:
            rtl_dongle_identifier = self.recv(12)
            return
        new_data_buffer = self.recv(16348)
        if CONFIG.watchdog_interval:
            watchdog_data_count += 16348
        if CONFIG.use_dsp_command:
            dsp_input_queue.put(new_data_buffer)
            # print("did put anyway")
        else:
            SERVER.add_data_to_clients(new_data_buffer)

    def writable(self):
        # check if any new commands to write
        return not commands.empty()

    def handle_write(self):
        while not commands.empty():
            mcmd = commands.get()
            self.send(mcmd)


def watchdog_thread():
    global rtl_tcp_connected
    global watchdog_data_count
    zero_buffer_size = 16348
    second_frac = 10
    zero_buffer = b'\x7f' * zero_buffer_size
    watchdog_data_count = 0
    rtl_tcp_connected = False
    null_fill = False
    time.sleep(4)  # wait before activating this thread
    LOGGER.info("watchdog started")
    first_start = True
    n = 0
    while True:
        wait_altogether = CONFIG.watchdog_interval if rtl_tcp_connected or first_start else CONFIG.reconnect_interval
        first_start = False
        if null_fill:
            LOGGER.error("watchdog: filling buffer with zeros.")
            while wait_altogether > 0:
                wait_altogether -= 1.0 / second_frac
                for i in range((2 * sample_rate) // (second_frac * zero_buffer_size)):
                    SERVER.add_data_to_clients(zero_buffer)
                    n += len(zero_buffer)
                    time.sleep(0)  # yield
                    if watchdog_data_count:
                        break
                if watchdog_data_count:
                    break
                time.sleep(1.0 / second_frac)
                # print("sent altogether",n)
        else:
            time.sleep(wait_altogether)
        null_fill = not watchdog_data_count
        if not watchdog_data_count:
            LOGGER.error("watchdog: restarting rtl_tcp_asyncore() now.")
            rtl_tcp_asyncore_reset(0)
        watchdog_data_count = 0


def dsp_debug_thread():
    global dsp_data_count
    while True:
        time.sleep(1)
        LOGGER.debug("DSP | Original data: %dkB/sec | Processed data: %dkB/sec", original_data_count / 1000, dsp_data_count / 1000)
        dsp_data_count = original_data_count = 0


def main():
    global server_missing_logged
    global rtl_dongle_identifier
    global original_data_count
    global dsp_input_queue
    global dsp_data_count
    global proc
    global commands
    global RTL_TCP, SERVER
    global sample_rate

    # set up logging
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    LOGGER.addHandler(stream_handler)
    file_handler = logging.FileHandler(CONFIG.log_file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)
    LOGGER.info("Server is UP")

    server_missing_logged = 0  # Not to flood the screen with messages related to rtl_tcp disconnect
    rtl_dongle_identifier = b''  # rtl_tcp sends some identifier on dongle type and gain values in the first few bytes right after connection
    dsp_data_count = original_data_count = 0
    commands = multiprocessing.Queue()
    dsp_input_queue = multiprocessing.Queue()
    sample_rate = 250000  # so far only watchdog thread uses it to fill buffer up with zeros on missing input

    # start dsp threads
    if CONFIG.use_dsp_command:
        LOGGER.info("Opening DSP process...")
        proc = subprocess.Popen(CONFIG.dsp_command.split(" "), stdin=subprocess.PIPE, stdout=subprocess.PIPE)  # !! should fix the split :-S
        dsp_read_thread_v = thread.start_new_thread(dsp_read_thread, ())
        dsp_write_thread_v = thread.start_new_thread(dsp_write_thread, ())
        if CONFIG.debug_dsp_command:
            dsp_debug_thread_v = thread.start_new_thread(dsp_debug_thread, ())

    # start watchdog thread
    if CONFIG.watchdog_interval != 0:
        watchdog_thread_v = thread.start_new_thread(watchdog_thread, ())

    # start asyncores
    RTL_TCP = RtlTcpAsyncore()
    SERVER = ServerAsyncore()

    asyncore.loop(0.1)


if __name__ == "__main__":
    print("RTL Multi-User Server v0.21a, made at HA5KFU Amateur Radio Club (http://ha5kfu.hu)")
    print("	code by Andras Retzler, HA7ILM")
    print("	distributed under GNU GPL v3")
    print()
    # === Load configuration script ===
    if len(sys.argv) == 1:
        print("Warning! Configuration script not specified. I will use: \"config_rtl.py\"")
        config_script = "config_rtl"
    else:
        config_script = sys.argv[1]
    CONFIG = __import__(config_script)
    if CONFIG.setuid_on_start:
        os.setuid(CONFIG.uid)
    main()
