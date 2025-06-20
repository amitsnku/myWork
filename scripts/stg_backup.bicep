param location string
param storageaccountname string
param containername string
param accounttype string
param kind string
param accesstier string
param minimumTlsVersion string
param supportsHttpsTrafficOnly bool
param publicNetworkAccess string
param allowBlobPublicAccess bool
param allowSharedKeyAccess bool
param allowCrossTenantReplication bool
param defaultOAuth bool
param networkAclsBypass string
param networkAclsDefaultAction string
param isHnsEnabled bool
param isSftpEnabled bool
param keySource string
param encryptionEnabled bool
param keyTypeForTableAndQueueEncryption string
param infrastructureEncryptionEnabled bool
param isBlobSoftDeleteEnabled bool
param blobSoftDeleteRetentionDays int
param isContainerSoftDeleteEnabled bool
param containerSoftDeleteRetentionDays int
param isShareSoftDeleteEnabled bool
param shareSoftDeleteRetentionDays int
param iprules string
param iprulesallow string
param defaultEncryptionScope string
param denyEncryptionScopeOverride bool
param resourceAccessResourceId string
param resourceAccessTenantId string
param virtualNetworkRuleId array
param virtualNetworkRuleAction string
param environment string
param subscriptionId string
//param virtualNetworkRuleState string


resource backupADLS 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageaccountname
  location: location
  properties: {
    accessTier: accesstier
    minimumTlsVersion: minimumTlsVersion
    supportsHttpsTrafficOnly: supportsHttpsTrafficOnly
    publicNetworkAccess: publicNetworkAccess
    allowBlobPublicAccess: allowBlobPublicAccess
    allowSharedKeyAccess: allowSharedKeyAccess
    allowCrossTenantReplication: allowCrossTenantReplication
    defaultToOAuthAuthentication: defaultOAuth
    networkAcls: {
      bypass: networkAclsBypass
      defaultAction: networkAclsDefaultAction
      ipRules: [
        {
          value: iprules
          action: iprulesallow
        }
      ]
      resourceAccessRules: [
        {
          resourceId: resourceAccessResourceId
          tenantId: resourceAccessTenantId
        }
      ]
      virtualNetworkRules: [for id in virtualNetworkRuleId: {
        id: id
        action: virtualNetworkRuleAction
        //state: virtualNetworkRuleState
      }]
    }
    isHnsEnabled: isHnsEnabled
    isSftpEnabled: isSftpEnabled
    encryption: {
      keySource: keySource
      services: {
        blob: {
          enabled: encryptionEnabled
        }
        file: {
          enabled: encryptionEnabled
        }
        table: {
          enabled: encryptionEnabled
          keyType: keyTypeForTableAndQueueEncryption
        }
        queue: {
          enabled: encryptionEnabled
          keyType: keyTypeForTableAndQueueEncryption
        }
      }
      requireInfrastructureEncryption: infrastructureEncryptionEnabled
    }
  }
  sku: {
    name: accounttype
  }
  kind: kind
  tags: {}
  dependsOn: []
}

resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2019-06-01' = {
  parent: backupADLS
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: isBlobSoftDeleteEnabled
      days: blobSoftDeleteRetentionDays
    }
    containerDeleteRetentionPolicy: {
      enabled: isContainerSoftDeleteEnabled
      days: containerSoftDeleteRetentionDays
    }
  }
}

resource fileServices 'Microsoft.Storage/storageAccounts/fileservices@2019-06-01' = {
  parent: backupADLS
  name: 'default'
  properties: {
    shareDeleteRetentionPolicy: {
      enabled: isShareSoftDeleteEnabled
      days: shareSoftDeleteRetentionDays
    }
  }
  dependsOn: [
    blobServices
  ]
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-05-01' = {
  parent: blobServices
  name: containername
  properties: {
    defaultEncryptionScope: defaultEncryptionScope
    denyEncryptionScopeOverride: denyEncryptionScopeOverride
    publicAccess: 'None'
  }
}



