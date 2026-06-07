// Registration entry for the native management surfaces.
import type { ExtensionServices } from "../services";
import { registerCatalogOps } from "./catalogOps";
import { registerProfileManagement } from "./profiles";
import { registerRunManagement } from "./runs";
import { registerScheduleManagement } from "./schedules";

export function registerManagement(services: ExtensionServices): void {
  registerProfileManagement(services);
  registerCatalogOps(services);
  registerRunManagement(services);
  registerScheduleManagement(services);
}
