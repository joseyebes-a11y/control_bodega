const form = document.getElementById("setup");
const feedback = document.getElementById("feedback");
const button = document.getElementById("submit");
window.MicroCellerDesktop.setupState().then(state => {
  document.getElementById("username").value = state.username;
  document.getElementById("username").readOnly = state.usernameLocked;
}).catch(error => { feedback.textContent = error.message; button.disabled = true; });
form.addEventListener("submit", async event => {
  event.preventDefault();
  const password = document.getElementById("password").value;
  if (password !== document.getElementById("repeat").value) { feedback.textContent = "Las contraseñas no coinciden."; return; }
  button.disabled = true; feedback.textContent = "Preparando tu bodega…";
  try {
    const result = await window.MicroCellerDesktop.setup({ username: document.getElementById("username").value, password });
    if (!result.ok) throw new Error(result.error);
  } catch (error) { feedback.textContent = error.message; button.disabled = false; }
});
