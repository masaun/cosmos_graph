import requests
import time
from logging import getLogger
import sys
import logging
from Queue import Queue
from py2neo import Graph

log = getLogger()
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
log.addHandler(ch)

seed_url = 'https://gaia-seeds.interblock.io/net_info'
queried = []

queue = Queue()

graph = Graph(host='localhost', user='neo4j', password='test1234')
graph.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")


def main():
    while True:
        time.sleep(0.5)

        try:
            # if queue is empty, we do query from origin
            if queue.empty():
                queue.put(('0', seed_url))

            # get one peer which we don't queried before
            centre_peer_id, query_url = queue.get()
            print query_url
            if query_url in queried:
                continue

            res = requests.get(query_url, timeout=10)
            result = res.json()
            peers = []
            for peer in result['result']['peers']:
                peer_id = peer['node_info']['id']
                # get the address
                addr = peer['node_info']['listen_addr'].split(':', 1)[0]

                # the peer is outbound
                if peer['is_outbound']:
                    rpc_addr = [element for element in peer['node_info']['other'] if 'rpc_addr' in element]
                    rpc_port = rpc_addr[0].rsplit(':', 1)[1] if rpc_addr else '26657'
                    rpc_url = 'http://{}:{}/net_info'.format(addr, rpc_port)

                    queue.put((peer_id, rpc_url))

                # store peers
                peers.append((peer_id, addr))

            # we do not record seed_url
            if query_url != seed_url:
                queried.append(query_url)

            log.debug(peers)
            graph.run('CREATE (peer:Peer {name:"' + centre_peer_id + '"}) RETURN peer')
            for peer_id, peer_ip in peers:
                graph.run('CREATE (peer:Peer {name:"' + peer_id + '"}) RETURN peer')
                graph.run(
                    'MATCH (n{name:"' + centre_peer_id + '"}),(m{name:"' + peer_id + '"}) CREATE (n)<-[r:CONN]-(m)')

        except Exception, e:
            log.exception(e.message)
            continue


if __name__ == '__main__':
    main()
