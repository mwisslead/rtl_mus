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

<<<<<<< HEAD
from __future__ import print_function, unicode_literals
=======
from __future__ import print_function
>>>>>>> d5e6c10 (Apply autopep8)
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


def ip_match(this, ip_ranges, for_allow):
    if not len(ip_ranges):
        return 1  # empty list matches all ip addresses
    for ip_range in ip_ranges:
        # print(this[0:len(ip_range)], ip_range)
        if this[0:len(ip_range)] == ip_range:
            return 1
    return 0


def ip_access_control(ip):
    if not cfg.use_ip_access_control:
        return 1
    allowed = 0
    if cfg.order_allow_deny:
        if ip_match(ip, cfg.allowed_ip_ranges, 1):
            allowed = 1
        if ip_match(ip, cfg.denied_ip_ranges, 0):
            allowed = 0
    else:
        if ip_match(ip, cfg.denied_ip_ranges, 0):
            allowed = 0
        if ip_match(ip, cfg.allowed_ip_ranges, 1):
            allowed = 1
    return allowed


def add_data_to_clients(new_data):
    # might be called from:
    # -> dsp_read
    # -> rtl_tcp_asyncore.handle_read
    clients_mutex.acquire()
    for client in clients:
        # print("client %d size: %d"%(client.ident,client.waiting_data.qsize()))
        if client.waiting_data:
            if client.waiting_data.full():
                if cfg.cache_full_behaviour == 0:
                    LOGGER.error("client cache full, dropping samples: " + str(client.ident) + "@" + client.socket[1][0])
                    while not client.waiting_data.empty():  # clear queue
                        client.waiting_data.get(False, None)
                elif cfg.cache_full_behaviour == 1:
                    # rather closing client:
                    LOGGER.error("client cache full, dropping client: " + str(client.ident) + "@" + client.socket[1][0])
                    client.close()
                elif cfg.cache_full_behaviour == 2:
                    pass  # client cache full, just not taking care
                else:
                    LOGGER.error("invalid value for cfg.cache_full_behaviour")
            else:
                client.waiting_data.put(new_data)
    clients_mutex.release()


def dsp_read_thread():
    global dsp_data_count
    while True:
        try:
            my_buffer = proc.stdout.read(1024)
        except IOError:
            LOGGER.error("DSP subprocess is not ready for reading.")
            time.sleep(1)
            continue
        add_data_to_clients(my_buffer)
        if cfg.debug_dsp_command:
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
        if cfg.debug_dsp_command:
            original_data_count += len(my_buffer)


class ClientHandler(asyncore.dispatcher):

    def __init__(self, client_param):
        self.client = client_param
        self.client.asyncore = self
        self.sent_dongle_id = False
        self.last_waiting_buffer = b""
        asyncore.dispatcher.__init__(self, self.client.socket[0])

    def handle_read(self):
        new_command = self.recv(5)
        if len(new_command) >= 5:
            if handle_command(bytearray(new_command), self.client):
                commands.put(new_command)

    def handle_error(self):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        LOGGER.info("client error: " + str(self.client.ident) + "@" + self.client.socket[1][0])
        LOGGER.exception(exc_value)
        self.close()

    def handle_close(self):
        self.client.close()
        LOGGER.info("client disconnected: " + str(self.client.ident) + "@" + self.client.socket[1][0])

    def writable(self):
        # print("queryWritable",not self.client.waiting_data.empty())
        return not self.client.waiting_data.empty()

    def handle_write(self):
        if not self.sent_dongle_id:
            self.send(rtl_dongle_identifier)
            self.sent_dongle_id = True
            return
        # print("write2client",self.client.waiting_data.qsize())
        next = self.last_waiting_buffer + self.client.waiting_data.get()
        sent = asyncore.dispatcher.send(self, next)
        self.last_waiting_buffer = next[sent:]


class ServerAsyncore(asyncore.dispatcher):

    def __init__(self):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((cfg.my_ip, cfg.my_listening_port))
        self.listen(5)
        LOGGER.info("Server listening on port: " + str(cfg.my_listening_port))

    def handle_accept(self):
        global max_client_id
        client = Client()
        client.socket = self.accept()
        if client.socket is None:  # not sure if required
            return
        if ip_access_control(client.socket[1][0]):
            client.ident = max_client_id
            max_client_id += 1
            client.start_time = time.time()
            client.waiting_data = multiprocessing.Queue(250)
            clients_mutex.acquire()
            clients.append(client)
            clients_mutex.release()
            handler = ClientHandler(client)
            LOGGER.info("client accepted: " + str(len(clients) - 1) + "@" + client.socket[1][0] + ":" + str(client.socket[1][1]) + "  users now: " + str(len(clients)))
        else:
            LOGGER.info("client denied: " + str(len(clients) - 1) + "@" + client.socket[1][0] + ":" + str(client.socket[1][1]) + " blocked by ip")
            client.socket.close()


rtl_tcp_resetting = False  # put me away


def rtl_tcp_asyncore_reset(timeout):
    global rtl_tcp_core
    global rtl_tcp_resetting
    if rtl_tcp_resetting:
        return
    # print("rtl_tcp_asyncore_reset")
    rtl_tcp_resetting = True
    time.sleep(timeout)
    try:
        rtl_tcp_core.close()
    except Exception:
        pass
    try:
        del rtl_tcp_core
    except Exception:
        pass
    rtl_tcp_core = RtlTcpAsyncore()
    # print(asyncore.socket_map)
    rtl_tcp_resetting = False


class RtlTcpAsyncore(asyncore.dispatcher):
    def __init__(self):
        asyncore.dispatcher.__init__(self)
        self.ok = True
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.connect((cfg.rtl_tcp_host, cfg.rtl_tcp_port))
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
        if cfg.watchdog_interval:
            watchdog_data_count += 16348
        if cfg.use_dsp_command:
            dsp_input_queue.put(new_data_buffer)
            # print("did put anyway")
        else:
            add_data_to_clients(new_data_buffer)

    def writable(self):
        # check if any new commands to write
        return not commands.empty()

    def handle_write(self):
        while not commands.empty():
            mcmd = commands.get()
            self.send(mcmd)


def handle_command(command, client):
    global sample_rate
    param = array.array("I", command[1:5])[0]
    param = socket.ntohl(param)
    command_id = command[0]
    client_info = str(client.ident) + "@" + client.socket[1][0] + ":" + str(client.socket[1][1])
    if time.time() - client.start_time < cfg.client_cant_set_until and not (cfg.first_client_can_set and client.ident == 0):
        LOGGER.info("deny: " + client_info + " -> client can't set anything until " + str(cfg.client_cant_set_until) + " seconds")
        return 0
    if command_id == 1:
        if max(map((lambda r: param >= r[0] and param <= r[1]), cfg.freq_allowed_ranges)):
            LOGGER.debug("allow: " + client_info + " -> set freq " + str(param))
            return 1
        else:
            LOGGER.debug("deny: " + client_info + " -> set freq - out of range: " + str(param))
    elif command_id == 2:
        LOGGER.debug("deny: " + client_info + " -> set sample rate: " + str(param))
        sample_rate = param
        return 0  # ordinary clients are not allowed to do this
    elif command_id == 3:
        LOGGER.debug("deny/allow: " + client_info + " -> set gain mode: " + str(param))
        return cfg.allow_gain_set
    elif command_id == 4:
        LOGGER.debug("deny/allow: " + client_info + " -> set gain: " + str(param))
        return cfg.allow_gain_set
    elif command_id == 5:
        LOGGER.debug("deny: " + client_info + " -> set freq correction: " + str(param))
        return 0
    elif command_id == 6:
        LOGGER.debug("deny/allow: set if stage gain")
        return cfg.allow_gain_set
    elif command_id == 7:
        LOGGER.debug("deny: set test mode")
        return 0
    elif command_id == 8:
        LOGGER.debug("deny/allow: set agc mode")
        return cfg.allow_gain_set
    elif command_id == 9:
        LOGGER.debug("deny: set direct sampling")
        return 0
    elif command_id == 10:
        LOGGER.debug("deny: set offset tuning")
        return 0
    elif command_id == 11:
        LOGGER.debug("deny: set rtl xtal")
        return 0
    elif command_id == 12:
        LOGGER.debug("deny: set tuner xtal")
        return 0
    elif command_id == 13:
        LOGGER.debug("deny/allow: set tuner gain by index")
        return cfg.allow_gain_set
    else:
        LOGGER.debug("deny: " + client_info + " sent an ivalid command: " + str(param))
    return 0


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
        wait_altogether = cfg.watchdog_interval if rtl_tcp_connected or first_start else cfg.reconnect_interval
        first_start = False
        if null_fill:
            LOGGER.error("watchdog: filling buffer with zeros.")
            while wait_altogether > 0:
                wait_altogether -= 1.0 / second_frac
                for i in range((2 * sample_rate) // (second_frac * zero_buffer_size)):
                    add_data_to_clients(zero_buffer)
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
        LOGGER.debug("DSP | Original data: " + str(int(original_data_count / 1000)) + "kB/sec | Processed data: " + str(int(dsp_data_count / 1000)) + "kB/sec")
        dsp_data_count = original_data_count = 0


class Client:
    ident = None  # id
    to_close = False
    waiting_data = None
    start_time = None
    socket = None
    asyncore = None

    def close(self):
        clients_mutex.acquire()
        for client in clients:
            if client.ident == self.ident:
                try:
                    self.socket[0].close()
                except Exception:
                    pass
                try:
                    self.asyncore.close()
                    del self.asyncore
                except Exception:
                    pass
                if self.waiting_data:
                    self.waiting_data.close()
                    self.waiting_data = None
                break
        clients_mutex.release()


def main():
    global server_missing_logged
    global rtl_dongle_identifier
    global clients
    global clients_mutex
    global original_data_count
    global dsp_input_queue
    global dsp_data_count
    global proc
    global commands
    global max_client_id
    global rtl_tcp_core
    global sample_rate

    # set up logging
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    LOGGER.addHandler(stream_handler)
    file_handler = logging.FileHandler(cfg.log_file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)
    LOGGER.info("Server is UP")

    server_missing_logged = 0  # Not to flood the screen with messages related to rtl_tcp disconnect
    rtl_dongle_identifier = b''  # rtl_tcp sends some identifier on dongle type and gain values in the first few bytes right after connection
    clients = []
    dsp_data_count = original_data_count = 0
    commands = multiprocessing.Queue()
    dsp_input_queue = multiprocessing.Queue()
    clients_mutex = multiprocessing.Lock()
    max_client_id = 0
    sample_rate = 250000  # so far only watchdog thread uses it to fill buffer up with zeros on missing input

    # start dsp threads
    if cfg.use_dsp_command:
        LOGGER.info("Opening DSP process...")
        proc = subprocess.Popen(cfg.dsp_command.split(" "), stdin=subprocess.PIPE, stdout=subprocess.PIPE)  # !! should fix the split :-S
        dsp_read_thread_v = thread.start_new_thread(dsp_read_thread, ())
        dsp_write_thread_v = thread.start_new_thread(dsp_write_thread, ())
        if cfg.debug_dsp_command:
            dsp_debug_thread_v = thread.start_new_thread(dsp_debug_thread, ())

    # start watchdog thread
    if cfg.watchdog_interval != 0:
        watchdog_thread_v = thread.start_new_thread(watchdog_thread, ())

    # start asyncores
    rtl_tcp_core = RtlTcpAsyncore()
    server_core = ServerAsyncore()

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
    cfg = __import__(config_script)
    if cfg.setuid_on_start:
        os.setuid(cfg.uid)
    main()
