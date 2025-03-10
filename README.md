<!--
SPDX-FileCopyrightText: StorPool <support@storpool.com>
SPDX-License-Identifier: BSD-2-Clause
-->

# StorPool Proxmox VE integration watchdog

The documentation may be viewed [at its StorPool web home][repo].

# StorPool HA watchdog replacement

PVE clusters offer a [High Availability](https://pve.proxmox.com/pve-docs/chapter-ha-manager.html) feature that automatically migrates virtual machines when their host loses connectivity to the cluster. As part of this functionality, a watchdog service monitors cluster status on each host, and fences it by rebooting the host when it has been disconnected from the cluster for a period of time.

Since StorPool relies on its own internal clustering mechanism it can remain unaffected by partitions in PVE’s cluster, making these reboots undesirable, and potentially harmful with regards to storage performance and availability. To prevent such issues StorPool has developed a replacement watchdog service (sp-watchdog-mux), which stops any running virtual machines, and restarts only select services on the host.

For details on enabling the replacement watchdog service, see [Installing the StorPool Proxmox integration](https://kb.storpool.com/storpool_integrations/proxmox/install.html#pve-install).

[repo]: https://kb.storpool.com/storpool_integrations/proxmox/install.html#enable-storpool-s-hci-ha-watchdog "The documentation at StorPool"
