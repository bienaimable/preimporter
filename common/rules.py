import xml.etree.ElementTree as Tree

def copy_attribute_to_node(element, attribute_name):
    attribute_value = element.get(attribute_name)
    subelement = Tree.Element(attribute_name)
    subelement.text = attribute_value
    element.append(subelement)
    return element

def copy_node(element, source_name, destination_name):
    text = element.find(source_name).text
    subelement = Tree.Element(destination_name)
    subelement.text = text
    element.append(subelement)
    return element

def rename_nodes(element, source_name, destination_name):
    for node in element.iter(tag=source_name):
        node.tag = destination_name
    return element

def shorten_value(element, node_name, length):
    subelement = element.find(node_name)
    if subelement.text:
        subelement.text = subelement.text[:length]
    return element
