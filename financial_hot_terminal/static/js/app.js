document.addEventListener("submit", async (event) => {
  const form = event.target.closest("[data-ingest-form]");
  if (!form) return;
  event.preventDefault();
  const button = form.querySelector("button");
  const original = button.textContent;
  button.textContent = "刷新中";
  button.disabled = true;
  try {
    await fetch(form.action, { method: "POST" });
    window.location.reload();
  } finally {
    button.textContent = original;
    button.disabled = false;
  }
});

document.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-review-status]");
  if (!button) return;
  const card = button.closest("[data-hotspot-id]");
  const hotspotId = card.dataset.hotspotId;
  button.disabled = true;
  await fetch(`/api/review/${hotspotId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status: button.dataset.reviewStatus, reviewer: "local" }),
  });
  card.remove();
});
