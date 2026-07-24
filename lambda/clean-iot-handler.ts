// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { IoTClient, ListPoliciesCommand, ListTargetsForPolicyCommand, DetachPolicyCommand,
  DeletePolicyCommand, DescribeThingGroupCommand, ListThingPrincipalsCommand, DetachThingPrincipalCommand,
  UpdateCertificateCommand, DeleteCertificateCommand, DeleteThingCommand, DeleteThingGroupCommand,
  ListRoleAliasesCommand, DeleteRoleAliasCommand } from '@aws-sdk/client-iot';
import { GreengrassV2Client, ListCoreDevicesCommand, DeleteCoreDeviceCommand,
  ListDeploymentsCommand, CancelDeploymentCommand, DeleteDeploymentCommand } from '@aws-sdk/client-greengrassv2';

const iot = new IoTClient();
const greengrassv2 = new GreengrassV2Client();

const MAX_RETRIES = 5;
const RETRY_BASE_DELAY_MS = 5000;

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function retryOnThrottle<T>(fn: () => Promise<T>): Promise<T> {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (e: any) {
      if (e.name === 'ThrottlingException' || e.Code === 'ThrottlingException') {
        const delay = RETRY_BASE_DELAY_MS * (2 ** attempt);
        console.log(`  Throttled. Waiting ${delay}ms before retry (${attempt + 1}/${MAX_RETRIES})...`);
        await sleep(delay);
      } else {
        throw e;
      }
    }
  }
  // Final attempt without catching
  return fn();
}

async function deletePolicies(farmName: string): Promise<void> {
  console.log('Getting IoT policies in the account');
  try {
    const response = await iot.send(new ListPoliciesCommand({}));
    const policies = response.policies || [];

    for (const policy of policies) {
      const policyName = policy.policyName!;
      if (policyName.includes(farmName)) {
        console.log(`  Getting targets for policy ${policyName}`);
        const targets = (await iot.send(
          new ListTargetsForPolicyCommand({ policyName }))).targets || [];

        for (const target of targets) {
          console.log(`  Detaching policy ${policyName} from target`);
          await iot.send(new DetachPolicyCommand({ policyName, target }));
        }

        console.log(`  Deleting policy ${policyName}`);
        await iot.send(new DeletePolicyCommand({ policyName }));
      }
    }
  } catch (e: any) {
    console.log(`  Error processing policies: ${e.message}`);
  }
}

async function deleteCoreDevices(farmName: string, thingGroupArn: string): Promise<void> {
  console.log('Getting core devices in the thing group');
  try {
    const response = await greengrassv2.send(
      new ListCoreDevicesCommand({ thingGroupArn }));
    const coreDevices = response.coreDevices || [];

    for (const coreDevice of coreDevices) {
      const thingName = coreDevice.coreDeviceThingName!;
      try {
        console.log(`  Getting principals for thing ${thingName}`);
        const principals = (await iot.send(
          new ListThingPrincipalsCommand({ thingName }))).principals || [];

        for (const principal of principals) {
          console.log(`  Detaching principal from thing ${thingName}`);
          await iot.send(new DetachThingPrincipalCommand({ thingName, principal }));

          if (principal.includes('cert')) {
            const certificateId = principal.split('cert/')[1];
            console.log(`  Deactivating certificate ${certificateId}`);
            await iot.send(new UpdateCertificateCommand({
              certificateId, newStatus: 'INACTIVE' }));
            console.log(`  Deleting certificate ${certificateId}`);
            await iot.send(new DeleteCertificateCommand({ certificateId }));
          }
        }

        console.log(`  Deleting core device and thing ${thingName}`);
        await greengrassv2.send(new DeleteCoreDeviceCommand({ coreDeviceThingName: thingName }));
        await iot.send(new DeleteThingCommand({ thingName }));
      } catch (e: any) {
        console.log(`  Error processing thing ${thingName}: ${e.message}`);
      }
    }
  } catch (e: any) {
    console.log(`  Error listing core devices: ${e.message}`);
  }
}

async function deleteThingGroup(farmName: string): Promise<void> {
  console.log(`Deleting thing group ${farmName}`);
  try {
    await iot.send(new DeleteThingGroupCommand({ thingGroupName: farmName }));
  } catch (e: any) {
    console.log(`  Error deleting thing group: ${e.message}`);
  }
}

async function deleteRoleAliases(farmName: string): Promise<void> {
  console.log('Getting role aliases');
  try {
    const response = await iot.send(new ListRoleAliasesCommand({}));
    const roleAliases = response.roleAliases || [];

    for (const roleAlias of roleAliases) {
      if (roleAlias.startsWith(farmName)) {
        console.log(`  Deleting role alias ${roleAlias}`);
        await iot.send(new DeleteRoleAliasCommand({ roleAlias }));
      }
    }
  } catch (e: any) {
    console.log(`  Error processing role aliases: ${e.message}`);
  }
}

async function deleteDeployments(thingGroupArn: string): Promise<void> {
  console.log('Getting Greengrass deployments');
  const deployments: any[] = [];
  try {
    let nextToken: string | undefined;
    do {
      const response: any = await greengrassv2.send(
        new ListDeploymentsCommand({ targetArn: thingGroupArn, historyFilter: 'ALL', nextToken }));
      deployments.push(...(response.deployments || []));
      nextToken = response.nextToken;
    } while (nextToken);
  } catch (e: any) {
    console.log(`  Error listing deployments: ${e.message}`);
    return;
  }

  console.log(`  Found ${deployments.length} deployment(s) to delete`);
  for (const deployment of deployments) {
    const deploymentId = deployment.deploymentId!;
    try {
      console.log(`  Canceling deployment ${deploymentId}`);
      await retryOnThrottle(() =>
        greengrassv2.send(new CancelDeploymentCommand({ deploymentId })));
      console.log(`  Deleting deployment ${deploymentId}`);
      await retryOnThrottle(() =>
        greengrassv2.send(new DeleteDeploymentCommand({ deploymentId })));
      await sleep(2000);
    } catch (e: any) {
      console.log(`  Error processing deployment ${deploymentId}: ${e.message}`);
    }
  }
}

export async function handler(event: any): Promise<any> {
  console.log('Event:', JSON.stringify(event));

  const requestType = event.RequestType;
  const farmName = event.ResourceProperties.FarmName;

  // Only run cleanup on Delete
  if (requestType !== 'Delete') {
    console.log(`RequestType is ${requestType}, nothing to do.`);
    return { PhysicalResourceId: farmName };
  }

  console.log(`Cleaning up IoT resources for ${farmName}`);

  // Get the thing group ARN
  let thingGroupArn: string;
  try {
    const response = await iot.send(new DescribeThingGroupCommand({ thingGroupName: farmName }));
    thingGroupArn = response.thingGroupArn!;
  } catch (e: any) {
    if (e.name === 'ResourceNotFoundException') {
      console.log(`Thing group ${farmName} not found. Constructing ARN.`);
      const region = process.env.AWS_REGION;
      const accountId = event.ServiceToken.split(':')[4];
      thingGroupArn = `arn:aws:iot:${region}:${accountId}:thinggroup/${farmName}`;
    } else {
      throw e;
    }
  }

  await deletePolicies(farmName);
  await deleteCoreDevices(farmName, thingGroupArn);
  await deleteThingGroup(farmName);
  await deleteRoleAliases(farmName);
  await deleteDeployments(thingGroupArn);

  console.log('Clean-up complete.');
  return { PhysicalResourceId: farmName };
}
