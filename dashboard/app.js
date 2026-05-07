/* ─── Shared chart defaults ───────────────────── */
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.color = '#9CA3AF';

/* ═══════════════════════════════════════════════
   Tab switching
═══════════════════════════════════════════════ */
function switchTab(tab) {
  document.querySelectorAll('.filter-tab').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.filter-controls').forEach(el => el.classList.add('hidden'));

  document.getElementById(`tab-${tab}`).classList.add('active');
  document.getElementById(`filter-${tab}`).classList.remove('hidden');
}

/* ═══════════════════════════════════════════════
   Chart helpers
═══════════════════════════════════════════════ */
const months = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
                 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];

const baseChartOptions = (color) => ({
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: 'index', intersect: false },
  plugins: {
    legend: { display: false },
    tooltip: {
      backgroundColor: '#111827',
      titleColor: '#fff',
      bodyColor: '#D1D5DB',
      padding: 10,
      borderRadius: 8,
    }
  },
  scales: {
    x: {
      grid: { display: false },
      ticks: { font: { size: 10 } }
    },
    y: {
      display: false,
      grid: { display: false }
    }
  },
  elements: {
    point: { radius: 3, hoverRadius: 6, borderWidth: 2, backgroundColor: '#fff' },
    line:  { tension: 0.42, borderWidth: 2.5 }
  }
});

/* ═══════════════════════════════════════════════
   1. Ticket Médio chart
═══════════════════════════════════════════════ */
const realTicket = [76.2, 78.5, 80.1, 79.6, 82.3, 83.8, 81.9, 84.4, 85.2, 86.0, 87.1, 87.4];
const metaTicket = [80, 80, 81, 81, 82, 82, 83, 83, 83, 84, 84, 83.9];

const ctxTicket = document.getElementById('chart-ticket').getContext('2d');
const gradTicket = ctxTicket.createLinearGradient(0, 0, 0, 90);
gradTicket.addColorStop(0, 'rgba(124,58,237,0.25)');
gradTicket.addColorStop(1, 'rgba(124,58,237,0)');

new Chart(ctxTicket, {
  type: 'line',
  data: {
    labels: months,
    datasets: [
      {
        label: 'Real',
        data: realTicket,
        borderColor: '#7C3AED',
        backgroundColor: gradTicket,
        fill: true,
      },
      {
        label: 'Meta',
        data: metaTicket,
        borderColor: '#D1D5DB',
        backgroundColor: 'transparent',
        borderDash: [5, 4],
        fill: false,
      }
    ]
  },
  options: baseChartOptions('#7C3AED')
});

/* ═══════════════════════════════════════════════
   2. Desvio Rent. Op. chart
═══════════════════════════════════════════════ */
const realRent = [10.2, 9.8, 9.5, 9.0, 8.8, 8.6, 8.5, 8.3, 8.4, 8.3, 8.2, 8.2];
const metaRent = [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10];

const ctxRent = document.getElementById('chart-rent').getContext('2d');
const gradRent = ctxRent.createLinearGradient(0, 0, 0, 90);
gradRent.addColorStop(0, 'rgba(14,165,233,0.2)');
gradRent.addColorStop(1, 'rgba(14,165,233,0)');

new Chart(ctxRent, {
  type: 'line',
  data: {
    labels: months,
    datasets: [
      {
        label: 'Real (%)',
        data: realRent,
        borderColor: '#0EA5E9',
        backgroundColor: gradRent,
        fill: true,
      },
      {
        label: 'Meta (%)',
        data: metaRent,
        borderColor: '#D1D5DB',
        backgroundColor: 'transparent',
        borderDash: [5, 4],
        fill: false,
      }
    ]
  },
  options: baseChartOptions('#0EA5E9')
});

/* ═══════════════════════════════════════════════
   3. Desvio Faturamento chart
═══════════════════════════════════════════════ */
const realFat = [10.1, 10.4, 10.7, 10.9, 11.2, 11.5, 11.3, 11.8, 11.9, 12.1, 12.3, 12.4];
const metaFat = [9.8, 10.0, 10.2, 10.4, 10.6, 10.8, 10.9, 11.1, 11.3, 11.5, 11.8, 12.1];

const ctxFat = document.getElementById('chart-fat').getContext('2d');
const gradFat = ctxFat.createLinearGradient(0, 0, 0, 90);
gradFat.addColorStop(0, 'rgba(16,185,129,0.22)');
gradFat.addColorStop(1, 'rgba(16,185,129,0)');

new Chart(ctxFat, {
  type: 'line',
  data: {
    labels: months,
    datasets: [
      {
        label: 'Real (R$M)',
        data: realFat,
        borderColor: '#10B981',
        backgroundColor: gradFat,
        fill: true,
      },
      {
        label: 'Meta (R$M)',
        data: metaFat,
        borderColor: '#D1D5DB',
        backgroundColor: 'transparent',
        borderDash: [5, 4],
        fill: false,
      }
    ]
  },
  options: baseChartOptions('#10B981')
});
