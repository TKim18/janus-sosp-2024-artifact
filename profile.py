"""For the SOSP Ceridwen artifact evaluation."""

# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as pg
# Import the Emulab specific extensions.
import geni.rspec.emulab as emulab

# Create a portal object,
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

imageList = (
            'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU18-64-STD',
            'urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU18-64-STD',
            'urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU16-64-STD',
            'urn:publicid:IDN+clemson.cloudlab.us+image+redundancy-PG0:compute',
            'urn:publicid:IDN+clemson.cloudlab.us+image+redundancy-PG0:compute:1',
            'urn:publicid:IDN+clemson.cloudlab.us+image+redundancy-PG0:maple',
            'urn:publicid:IDN+clemson.cloudlab.us+image+emulab-ops//UBUNTU18-PPC64LE',
            'urn:publicid:IDN+clemson.cloudlab.us+image+tplearn-PG0//ibm8335_tf_installed',
            'urn:publicid:IDN+apt.emulab.net+image+redundancy-PG0:osdipreacthdfsdep'

    )

pc.defineParameter("nNodes", "Number of computation nodes",
                   portal.ParameterType.INTEGER, 25)

pc.defineParameter("osImage", "Select OS image",
                   portal.ParameterType.IMAGE,
                   imageList[8], imageList)

pc.defineParameter("nodeType", "The type of node",
                   portal.ParameterType.STRING, "r320")


params = pc.bindParameters()


nodes = []
lan = request.LAN("lan")

for i in range(params.nNodes):
    node = request.RawPC('node'+str(i))
    nodes.append(node)
    intf = node.addInterface()

    node.routable_control_ip = True
    if params.nodeType != "None":
        node.hardware_type = params.nodeType
    node.disk_image = params.osImage
    # node.addService(pg.Execute("sh", "sudo bash /local/repository/general.sh"))
    # bs = node.Blockstore("bs", "/data")
    # bs.size = "300GB"
    lan.addInterface(intf)


# link = request.Link(members = nodes)
# link.best_effort = True
# link.vlan_tagging = True

# Print the generated rspec
pc.printRequestRSpec(request)