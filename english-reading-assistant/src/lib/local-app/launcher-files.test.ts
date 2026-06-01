import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

const root = process.cwd();

describe("local desktop app launchers", () => {
  it("exposes an app-window mode through auto-start.bat", () => {
    const launcher = readFileSync(join(root, "auto-start.bat"), "utf8");

    expect(launcher).toContain('if /I "%MODE%"=="app" goto launch_app');
    expect(launcher).toContain(":open_app_window");
    expect(launcher).toContain("--app=http://localhost:%PORT%");
  });

  it("includes an installer that creates desktop and start menu shortcuts", () => {
    const installerPath = join(root, "install-local-app.bat");
    expect(existsSync(installerPath)).toBe(true);

    const installer = readFileSync(installerPath, "utf8");
    expect(installer).toContain("CreateShortcut");
    expect(installer).toContain("Desktop");
    expect(installer).toContain("Start Menu");
    expect(installer).toContain("auto-start.bat");
    expect(installer).toContain("app");
  });
});
