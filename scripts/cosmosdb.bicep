/***parameters***/
param location string = resourceGroup().location
param stgacc_name string='outitandevadlsgen2'
param containerName string='outitandevadls'
param directoryName string = 'AskOU1'
param titansubcriptionId string='8590ce05-227b-4589-9c61-07caef7a75a5'
param askousubscrptionID string='2432daba-a358-4fc9-94a0-c118ee4b6146'
param databricksWorkspaceName string='outitandevadbnedw'
param databricksResourceGroup string='ou-titan-adb-dev-rg-ne'
param adfResourceGroup string='ou-titan-adf-dev-rg-ne'
param uamiName string='ou-titan-askou-cosmos-dev-2'
param uamiResourceGroup string='ou-titan-adf-dev-rg-ne'
param roleDefinitionId string = 'b24988ac-6180-42a0-ab88-20f7382dd24c' // Contributor Role ID (Change as needed)
param adfName string='outitandevadfnetr'
param cosmosDbName string='ouaskoucosmosdbuks'
param cosmosDbResourceGroup string='ou-askou-dev'
param cosmosDbSubscriptionId string='2432daba-a358-4fc9-94a0-c118ee4b6146'
param cosmosdbroleDefinitionId string = 'b1f13f4b-d5e5-4b77-a828-6c3c5735a3c6'
param vnetName string='outitandevvnetne'
param vnetResourceGroup string='ou-titan-adb-dev-rg-ne'
param subnetName string='subnet_redis_poc'
param privateLinkServiceConnectionName string = 'CosmosDbPrivateLinkConnection'
param privateLinkServiceId string=''
// var roleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') 

// resource stg 'Microsoft.Storage/storageAccounts@2023-04-01' existing={
//   name:stgacc_name
// }

// resource createDirectory 'Microsoft.Resources/deploymentScripts@2023-08-01'={
//   name:'createDirectory'
//   kind:'AzureCLI'
//   location:location
//   properties:{
//     azCliVersion:'2.42.0'
//     retentionInterval:'P1D'
//     arguments: '\'${stgacc_name}\' \'${containerName}\'${directoryName}\''
//     scriptContent:'az storage fs directory create -n $3 -f $2 --account-name $1'
//     // scriptContent: 'az storage fs directory create --account-name $1 -f $2 -n $3'

//   }
// }

// Create user assigned managed identity
resource uami 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31'={
  location : location
  name: uamiName
  // resourceGroup: resourceGroup(uamiResourceGroup)
}

// Reference an existing Databricks workspace
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' existing = {
  name: databricksWorkspaceName
  // resourceGroup: databricksResourceGroup
}

// Assign a role to the UAMI on Databricks
resource roleAssignment_dbr 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(databricksWorkspace.id, roleDefinitionId,uami.id)
  scope: databricksWorkspace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleDefinitionId)
    principalId: uami.properties.principalId
  }
}
// Reference an existing Azure Data Factory
resource adf 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: adfName
  scope: resourceGroup(adfResourceGroup)
}


// Assign a role to the UAMI on the ADF
resource roleAssignment_adf 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(adf.id, roleDefinitionId,uami.id)
  // scope:adf.id
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleDefinitionId)
    principalId: uami.properties.principalId
  }
}

// Reference the existing Cosmos DB in a different subscription
resource cosmosDb 'Microsoft.DocumentDB/databaseAccounts@2023-03-15' existing = {
  name: cosmosDbName
  scope: resourceGroup(cosmosDbResourceGroup)
}

// Assign a role to the UAMI on the Cosmos DB account
resource roleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(uami.id, cosmosDb.id, roleDefinitionId)
  // scope: askoucosmosDb // Using Cosmos DB as the resource scope
  properties: {
    roleDefinitionId: subscriptionResourceId(cosmosDbSubscriptionId, 'Microsoft.Authorization/roleDefinitions', cosmosdbroleDefinitionId)
    principalId: uami.properties.principalId
  }
}
// Reference the Virtual Network where Databricks resides
resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' existing = {
  name: vnetName
  scope: resourceGroup(vnetResourceGroup)
}
// Create a Private Endpoint for Cosmos DB
resource privateEndpoint 'Microsoft.Network/privateEndpoints@2024-05-01' = {
  name: '${cosmosDbName}-privateEndpoint'
  // scope: databricksResourceGroup
  properties: {
    subnet: {
      id: '/subscriptions/8590ce05-227b-4589-9c61-07caef7a75a5/resourceGroups/ou-titan-adb-dev-rg-ne/providers/Microsoft.Network/virtualNetworks/outitandevvnetne/subnets/askou-subnet-dev'
    }
    privateLinkServiceConnections: [
      {
        name: privateLinkServiceConnectionName
        properties: {
          privateLinkServiceConnectionState: {
            status: 'Approved'
            description: 'Approved by Databricks'
          }
          privateLinkServiceConnection: {
            id: cosmosDb.id
          }
        }
      }
    ]
  }
}

// /subscriptions/8590ce05-227b-4589-9c61-07caef7a75a5/resourceGroups/ou-titan-adb-dev-rg-ne/providers/Microsoft.Network/virtualNetworks/outitandevvnetne/subnets/subnet_redis_poc
