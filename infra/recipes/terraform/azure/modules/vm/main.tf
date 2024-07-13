// define the virtual network
resource "azurerm_virtual_network" "virtual-network" {
  name                = var.virtual_network_name
  address_space       = ["10.0.0.0/16"]
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
}

// define the subnet, the specific area in the virtual network
resource "azurerm_subnet" "subnet" {
  name = var.subnet_name
  resource_group_name =  var.resource_group_name
  virtual_network_name = azurerm_virtual_network.virtual-network.name
  address_prefixes = ["10.0.1.0/24"]
}

// define the network interface, allowing the VM to connect to the network
resource "azurerm_network_interface" "nic" {
  name = var.network_interface_name
  location = var.resource_group_location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name = "config"
    subnet_id = azurerm_subnet.subnet.id
    private_ip_address_allocation = "Dynamic"
  }
}

// define the virtual machine
resource "azurerm_virtual_machine" "vm" {
    name = var.vm_name
    location = var.resource_group_location
    resource_group_name = var.resource_group_name
    network_interface_ids = [azurerm_network_interface.nic.id]
    vm_size = var.vm_size
    delete_os_disk_on_termination = true

    // Can add image here with storage_image_reference
    storage_image_reference {
      publisher = "Canonical"
      offer     = "UbuntuServer"
      sku       = "18.04-LTS"
      version   = "latest"
    }

    os_profile_linux_config {
      disable_password_authentication = false // force using admin password
    }

    storage_os_disk {
      name = "osdisk"
      create_option = "FromImage"
      caching = "ReadWrite"
      managed_disk_type = "Standard_LRS"
    }

    os_profile {
      computer_name = "host"
      admin_username = var.vm_username
      admin_password = var.vm_password
    }
}