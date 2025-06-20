/***parameters***/
// param location string = resourceGroup().location
param accessconn_dbr string='databricks-ac-outitandevadlsgen2'
param stgacc_name string='outitandevadlsgen2'
var roleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') 

resource stg 'Microsoft.Storage/storageAccounts@2023-04-01' existing={
  name:stgacc_name
}


resource accessconn 'Microsoft.Databricks/accessConnectors@2024-05-01' ={
  identity:{
    type:'SystemAssigned'
  }
  name:accessconn_dbr
  location:'northeurope'
}

output accessConn_rsrc_id string=accessconn.id
// var principalid=accessconn.identity.principalId

resource roleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01'= { 
  name: guid(roleDefinitionId,accessconn.name,stg.name) 
  properties: { 
    roleDefinitionId: roleDefinitionId
    principalId: accessconn.identity.principalId
    principalType: 'ServicePrincipal'
    
   } 
  }

