// define the virtual network
resource "azurerm_virtual_network" "sandbox" {
  name                = "sandbox-network"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.sandbox.location
  resource_group_name = azurerm_resource_group.sandbox.name
}

// define the subnet, the specific area in the virtual network
resource "azurerm_subnet" "sandbox" {
  name = "sandbox-subnet"
  resource_group_name =  azurerm_resource_group.sandbox.name
  virtual_network_name = azurerm_virtual_network.sandbox.name
  address_prefixes = ["10.0.1.0/24"]
}

// define the network interface, allowing the VM to connect to the network
resource "azurerm_network_interface" "sandbox" {
  name = "sandbox-nic"
  location = azurerm_resource_group.sandbox.location
  resource_group_name = azurerm_resource_group.sandbox.name

  ip_configuration {
    name = "config"
    subnet_id = azurerm_subnet.sandbox.id
    private_ip_address_allocation = "Dynamic"
  }
}

// defint the virtual machine
resource "azurerm_virtual_machine" "sandbox" {
    name = "sandbox-vm"
    location = azurerm_resource_group.sandbox.location
    resource_group_name = azurerm_resource_group.sandbox.name
    network_interface_ids = [azurerm_network_interface.sandbox.id]
    vm_size = "Standard_DS1_v2"

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
      admin_username = var.username
      admin_password = var.password
    }
}