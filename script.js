function getElement(selector) {
  return document.querySelector(selector);
}

const metricsGrid = getElement("#metrics-grid");
const tbody = getElement("#contracts-body");
const sourceList = getElement("#source-list");
const featureGrid = getElement("#feature-grid");
const pipelineSteps = getElement("#pipeline-steps");
const systemList = getElement("#system-list");
const roadmapList = getElement("#roadmap-list");
const readinessGrid = getElement("#readiness-grid");
const viewFilter = getElement("#view-filter");
const dayFilter = getElement("#day-filter");
const sideFilter = getElement("#side-filter");
const searchFilter = getElement("#search-filter");
const filterSummary = getElement("#filter-summary");
let dashboardState = null;

if (viewFilter) {
  viewFilter.value = "all";
}

if (dayFilter) {
  dayFilter.value = "today";
}

function setHtml(element, html) {
  if (!element) {
    return;
  }

  element.innerHTML = html;
}

function formatLocalDateKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addLocalDays(dateString, offset) {
  const date = new Date(`${dateString}T12:00:00`);
  date.setDate(date.getDate() + offset);
  return formatLocalDateKey(date);
}

function sortFilteredContracts(contracts) {
  return [...contracts].sort((left, right) => {
    if (left.eventDate !== right.eventDate) {
      return (left.eventDate || "").localeCompare(right.eventDate || "");
    }

    return (right.displayRankScore ?? -Infinity) - (left.displayRankScore ?? -Infinity);
  });
}

function renderMetrics(metrics) {
  setHtml(
    metricsGrid,
    metrics
      .map(
        (metric) => `
          <article class="metric-card">
            <p class="eyebrow">${metric.eyebrow}</p>
            <div class="metric-value ${metric.valueClass}">${metric.value}</div>
            <p class="metric-subtle">${metric.subtle}</p>
          </article>
        `
      )
      .join("")
  );
}

function renderContracts(contracts) {
  if (!contracts.length) {
    setHtml(
      tbody,
      `
        <tr>
          <td colspan="10" class="empty-cell">No contracts match the current filters.</td>
        </tr>
      `
    );
    return;
  }

  setHtml(
    tbody,
    contracts
      .map(
        (row) => `
          <tr class="${row.rowClass || ""}">
            <td>
              <div class="contract-title mono ${row.setupClass || ""}">${row.contract}</div>
              <div class="contract-subtitle">
                <span>${row.contractSubtitle || ""}</span>
                <span class="setup-pill ${row.setupClass || ""}">${row.setupLabel || ""}</span>
              </div>
            </td>
            <td class="mono">${row.location}</td>
            <td>${row.kalshiProbDisplay}</td>
            <td>${row.modelProbDisplay}</td>
            <td class="positive">${row.modelForecastDisplay}</td>
            <td>${row.contractCostDisplay || "--"}</td>
            <td>${row.payoutRatioDisplay}</td>
            <td class="mono">${row.signalDriver}</td>
            <td><span class="confidence-pill ${row.confidenceClass}">${row.confidenceLabel}</span></td>
            <td><a class="action-link" href="${row.inspectUrl || "#"}" target="_blank" rel="noreferrer">Inspect ↗</a></td>
          </tr>
        `
      )
      .join("")
  );

}

function renderCards(target, cards, className, badgeClass = "") {
  setHtml(
    target,
    cards
      .map(
        (card) => `
          <article class="${className}">
            <div class="${className}-header">
              <h3>${card.title}</h3>
              <span class="${badgeClass}">${card.tag || card.stage}</span>
            </div>
            <p>${card.description}</p>
          </article>
        `
      )
      .join("")
  );
}

function renderSystems(systems) {
  setHtml(
    systemList,
    systems
      .map(
        (system) => `
          <article class="system-card">
            <h3>${system.label}</h3>
            <p>${system.value}</p>
          </article>
        `
      )
      .join("")
  );
}

function renderReadiness(items) {
  setHtml(
    readinessGrid,
    items
      .map(
        (item) => `
          <article class="readiness-card">
            <p class="eyebrow">${item.title}</p>
            <h3>${item.value}</h3>
            <p>${item.detail}</p>
          </article>
        `
      )
      .join("")
  );
}

async function loadDashboard() {
  if (!tbody) {
    throw new Error("Dashboard markup is missing #contracts-body");
  }

  const response = await fetch("/api/dashboard");
  const dashboard = await response.json();
  dashboardState = dashboard;

  renderMetrics(dashboard.metrics);
  applyFilters();
  renderCards(sourceList, dashboard.dataSources, "source-card", "source-tag");
  renderCards(featureGrid, dashboard.featureCards, "feature-card", "feature-tag");
  renderSystems(dashboard.systems);
  renderCards(roadmapList, dashboard.modelRoadmap, "roadmap-card", "roadmap-tag");
  renderReadiness(dashboard.backtestReadiness);

  setHtml(
    pipelineSteps,
    dashboard.pipeline
      .map(
        (step, index) => `
          <article class="pipeline-step" data-step="0${index + 1}">
            <h3>${step.title}</h3>
            <p>${step.description}</p>
          </article>
        `
      )
      .join("")
  );

  const updatedAt = new Date(dashboard.updatedAt);
  const hours = String(updatedAt.getUTCHours()).padStart(2, "0");
  const minutes = String(updatedAt.getUTCMinutes()).padStart(2, "0");
  const updatedTime = getElement("#updated-time");
  if (updatedTime) {
    updatedTime.textContent = `${hours}:${minutes}`;
  }
}

function applyFilters() {
  if (!dashboardState) {
    return;
  }

  const viewValue = viewFilter?.value || "best";
  const dayValue = dayFilter?.value || "all";
  const sideValue = sideFilter?.value || "all";
  const searchValue = (searchFilter?.value || "").trim().toLowerCase();
  const source =
    viewValue === "all"
      ? dashboardState.contractViews?.allContracts || dashboardState.contracts
      : dashboardState.contractViews?.bestByCity || dashboardState.contracts;
  const localToday = formatLocalDateKey(new Date());
  const localTomorrow = addLocalDays(localToday, 1);

  const filtered = sortFilteredContracts(
    source.filter((row) => {
      if (dayValue === "today" && row.eventDate !== localToday) {
        return false;
      }
      if (dayValue === "tomorrow" && row.eventDate !== localTomorrow) {
        return false;
      }
      if (sideValue !== "all" && row.recommendedSide !== sideValue) {
        return false;
      }
      if (searchValue) {
        const haystack = `${row.location} ${row.contract} ${row.signal}`.toLowerCase();
        if (!haystack.includes(searchValue)) {
          return false;
        }
      }
      return true;
    })
  );

  renderContracts(filtered);

  if (filterSummary) {
    const viewLabel = viewValue === "all" ? "all contracts" : "best contract per city";
    const dayLabel =
      dayValue === "today" ? "today" : dayValue === "tomorrow" ? "tomorrow" : "today and tomorrow";
    const sideLabel = sideValue === "all" ? "both sides" : `${sideValue.toUpperCase()} side`;
    const searchLabel = searchValue ? ` matching "${searchValue}"` : "";
    const availabilityNote =
      dayValue === "today" && !filtered.length
        ? ` No contracts matched ${localToday}.`
        : dayValue === "tomorrow" && !filtered.length
          ? ` No contracts matched ${localTomorrow}.`
          : "";
    filterSummary.textContent = `${filtered.length} ${viewLabel} shown for ${dayLabel}, ${sideLabel}${searchLabel}.${availabilityNote}`;
  }
}

loadDashboard().catch((error) => {
  if (tbody) {
    tbody.innerHTML = `
      <tr>
        <td colspan="12" class="error-cell">Failed to load dashboard data: ${error.message}</td>
      </tr>
    `;
    return;
  }

  console.error("Failed to load dashboard data:", error);
});

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelector(".tab.is-active")?.classList.remove("is-active");
    tab.classList.add("is-active");
  });
});

[viewFilter, dayFilter, sideFilter].forEach((control) => {
  control?.addEventListener("change", applyFilters);
});

searchFilter?.addEventListener("input", applyFilters);
