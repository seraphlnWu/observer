# coding=utf8
#
#

'''
    Controller Module
'''

import os.path
import sys
sys.path.append(os.path.split(__file__)[0])

from observer.node.controller import BaseControllerNode
from observer.loaders import getObject
from twisted.application import service, internet
from twisted.spread import pb

import configurations.mainnode as mainnodecfg
import configurations as servercfg
import configurations.api as apicfg

from twisted.conch.manhole_tap import Options, makeService

application = service.Application('Observer Spider Controller')

if servercfg.app_path:
    app_path = os.path.dirname(__file__)
    servercfg.app_path = app_path
else:
    app_path = None

nodeservice = getObject('observer.platform.controller.ControllerNode')(cfg=mainnodecfg)
mainnode = BaseControllerNode(nodeservice)

internet.TCPServer(mainnodecfg.host, pb.PBServerFactory(mainnode)).setServiceParent(application)
nodeservice.setServiceParent(application)


o = Options()
o.parseOptions(["--passwd", "manholeusers.txt", "--sshPort", mainnodecfg.mainnode_manhole_port])


namespace = {}
namespace['application'] = application
namespace['services'] = service.IServiceCollection(application)
namespace['nodeservice'] = nodeservice
o['namespace'] = namespace

manhole_service = makeService(o)
manhole_service.setServiceParent(application)