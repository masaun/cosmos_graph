import requests
import time
from logging import getLogger
import sys
import logging
from Queue import Queue

log = getLogger()
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
log.addHandler(ch)

seed_url = 'https://gaia-seeds.interblock.io/net_info'
queried = []


queue = Queue()

from neo4jrestclient.client import GraphDatabase
url = "http://neo4j:test1234@localhost:7474/db/data/"
gdb = GraphDatabase(url)
# All Delete
gdb.query("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r", data_contents=True)

def main():
    while True:
        time.sleep(0.5)

        try:
            # if queue is empty, we do query from origin
            if queue.empty():
                queue.put((0,seed_url))

            # get one peer which we don't queried before
            peer_id, query_url = queue.get()
            print query_url
            if query_url in queried:
                continue

            res = requests.get(query_url, timeout=10)
            result = res.json()
            peers = []
            for peer in result['result']['peers']:
                # get the address
                addr = peer['node_info']['listen_addr'].split(':', 1)[0]

                # the peer is outbound
                if peer['is_outbound']:
                    rpc_addr = [element for element in peer['node_info']['other'] if 'rpc_addr' in element]
                    rpc_port = rpc_addr[0].rsplit(':', 1)[1] if rpc_addr else '26657'
                    rpc_url = 'http://{}:{}/net_info'.format(addr, rpc_port)
                    peer_id = peer['node_info']['id']
                    queue.put((peer_id, rpc_url))

                # store peers
                peers.append((peer_id, addr))

            # we do not record seed_url
            if query_url != seed_url:
                queried.append(query_url)

            log.debug(peers)
            centre_peer = gdb.nodes.create(id=peer_id)
            for peer_id, peer_ip in peers:
                peer_node = gdb.nodes.create(id=peer_id, ip=peer_ip)
                centre_peer.relationships.create("link", peer_node)
                peer_node.relationships.create("link", centre_peer)

        except Exception, e:
            log.debug(e.message)
            continue

if __name__ == '__main__':
    main()
