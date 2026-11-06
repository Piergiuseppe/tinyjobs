(function () {
  const INTERVAL = 5000;

  const address = new URL(location.href);
  if (address.searchParams.has("notice")) {
    address.searchParams.delete("notice");
    address.searchParams.delete("job");
    history.replaceState(null, "", address.pathname + address.search + address.hash);
  }

  function edited(form) {
    return Array.from(form.elements).some((control) => {
      if (control.tagName === "SELECT") {
        const options = Array.from(control.options);
        const initial = Math.max(0, options.findIndex((option) => option.defaultSelected));
        return control.selectedIndex !== initial;
      }
      return control.tagName === "INPUT" && control.value !== control.defaultValue;
    });
  }

  function busy() {
    const active = document.activeElement;
    const main = document.querySelector("main");
    const selected = window.getSelection && window.getSelection().toString();
    return (
      document.hidden ||
      Boolean(selected) ||
      Boolean(active && main && main.contains(active) && active.matches("input, select, textarea, button, :focus-visible")) ||
      Array.from(document.querySelectorAll("main form")).some(edited)
    );
  }

  function reveal(hash) {
    if (!hash || hash.length < 2) {
      return;
    }
    const target = document.getElementById(hash.slice(1));
    if (target && target.tagName === "DETAILS") {
      target.open = true;
    }
  }

  document.addEventListener("click", (event) => {
    const anchor = event.target.closest && event.target.closest('a[href^="#"]');
    if (anchor) {
      reveal(anchor.getAttribute("href"));
    }
  });

  window.addEventListener("hashchange", () => reveal(location.hash));

  window.addEventListener("pageshow", (event) => {
    if (event.persisted) {
      document.querySelectorAll("form.filters").forEach((form) => form.reset());
    }
  });

  reveal(location.hash);

  async function refresh() {
    if (busy()) {
      return;
    }
    let response;
    try {
      response = await fetch(location.href, { headers: { Accept: "text/html" }, cache: "no-store" });
    } catch (error) {
      return;
    }
    if (!response.ok) {
      return;
    }
    const text = await response.text();
    if (busy()) {
      return;
    }
    const fresh = new DOMParser().parseFromString(text, "text/html").querySelector("main");
    const current = document.querySelector("main");
    if (!fresh || !current) {
      return;
    }
    const open = new Set(Array.from(current.querySelectorAll("details[open]")).map((detail) => detail.id));
    fresh.querySelectorAll("details").forEach((detail) => {
      if (open.has(detail.id)) {
        detail.open = true;
      }
    });
    const notice = current.querySelector(".notice");
    const heading = fresh.querySelector("h1");
    if (notice && heading && !fresh.querySelector(".notice")) {
      heading.after(notice);
    }
    current.replaceWith(fresh);
  }

  function schedule() {
    setTimeout(async () => {
      await refresh();
      schedule();
    }, INTERVAL);
  }

  schedule();
})();
