const appUrlInput = document.getElementById("appUrl");
const tokenInput = document.getElementById("token");
const statusEl = document.getElementById("status");
const saveButton = document.getElementById("save");
const importButton = document.getElementById("import");

chrome.storage.sync.get(["appUrl", "token"], (values) => {
  appUrlInput.value = values.appUrl || "";
  tokenInput.value = values.token || "";
});

saveButton.addEventListener("click", async () => {
  await chrome.storage.sync.set({
    appUrl: trimTrailingSlash(appUrlInput.value),
    token: tokenInput.value.trim(),
  });
  setStatus("配置已保存。");
});

importButton.addEventListener("click", async () => {
  try {
    setStatus("正在读取当前页面...");
    const appUrl = trimTrailingSlash(appUrlInput.value);
    const token = tokenInput.value.trim();
    if (!appUrl || !token) {
      setStatus("请先填写应用地址和 Token。", true);
      return;
    }

    const [tab] = await chrome.tabs.query({
      active: true,
      currentWindow: true,
    });
    const [{ result }] = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      func: extractPage,
    });

    setStatus("正在提交到英读助手...");
    const response = await fetch(`${appUrl}/api/documents/from-url`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(result),
    });
    const payload = await response.json();
    if (!response.ok) {
      setStatus(payload.error?.message || "导入失败。", true);
      return;
    }

    setStatus(
      `已导入。<a href="${appUrl}/documents/${payload.id}" target="_blank">打开文章</a>`,
    );
  } catch (error) {
    setStatus(error.message || "导入失败。", true);
  }
});

function setStatus(message, isError = false) {
  statusEl.innerHTML = message;
  statusEl.style.color = isError ? "#b91c1c" : "#64706c";
}

function trimTrailingSlash(value) {
  return value.trim().replace(/\/+$/, "");
}

function extractPage() {
  const clone = document.cloneNode(true);
  clone
    .querySelectorAll("script,style,nav,footer,aside,form,noscript,iframe")
    .forEach((node) => node.remove());

  const main =
    clone.querySelector("article") ||
    clone.querySelector("main") ||
    clone.body;
  const title =
    document.querySelector("meta[property='og:title']")?.content ||
    document.title ||
    location.hostname;

  const paragraphs = Array.from(
    main.querySelectorAll("h1,h2,h3,p,li,blockquote"),
  )
    .map((node) => node.textContent.replace(/\s+/g, " ").trim())
    .filter((text) => text.length > 30 || /^[A-Z][^.!?]{3,80}$/.test(text));

  const text = [...new Set(paragraphs)].join("\n\n");

  return {
    title,
    url: location.href,
    text,
  };
}
