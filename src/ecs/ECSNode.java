/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ecs;
import ecs.IECSNode;
/**
 *
 * @author Joshua
 */
public class ECSNode implements IECSNode {
    
    private String nodeName;
    private String nodeHost;
    private int nodePort;
    private String[] nodeHashRange;
    
    public ECSNode(String _nodename, String _nodehost, int _nodeport, String[] _nodehashrange) {
        nodeName = _nodename;
        nodeHost = _nodehost;
        nodePort = _nodeport;
        nodeHashRange = _nodehashrange;
    }
    
    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost() {
        return nodeHost;
    }

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort() {
        return nodePort;
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange() {
        return nodeHashRange;
    }
    
}
