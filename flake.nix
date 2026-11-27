{
  description = "Cluster API bootstrap and control-plane providers for chalkos";

  # Follows chalkos's own nixpkgs and go-overlay, so the toolchain this devShell
  # builds is the one chalkos builds for itself, and both repositories move
  # together when chalkos's pin is bumped.
  inputs = {
    chalkos.url = "github:trevex/chalkos/46f275276f65a3a5fb980487a429b2315ec47c60";
    nixpkgs.follows = "chalkos/nixpkgs";
    go-overlay.follows = "chalkos/go-overlay";
  };

  outputs =
    {
      chalkos,
      nixpkgs,
      go-overlay,
      ...
    }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      # The Go toolchain go.mod names, the same way chalkos resolves its own.
      go = (go-overlay.lib.mkGoBin pkgs).fromGoModStrict ./go.mod;
    in
    {
      # The images of the KubeVirt lab in lab/: lab-image, lab-image-oci and lab-disk.
      packages.${system} = import ./nix/lab-images.nix { inherit chalkos pkgs; };

      devShells.${system}.default = pkgs.mkShell {
        packages = [
          go
          pkgs.kubernetes-controller-tools # controller-gen
          pkgs.kustomize
          pkgs.setup-envtest
          pkgs.kind
          pkgs.kubectl
          # clusterctl of the Cluster API release go.mod names, built as a go.mod tool, so the lab
          # installs the core release the providers build against. It runs inside the repository.
          (pkgs.writeShellScriptBin "clusterctl" ''exec go tool clusterctl "$@"'')
          pkgs.kubevirt # virtctl
          pkgs.just
          pkgs.crane
          pkgs.qemu # qemu-img
          pkgs.golangci-lint
          # Pushes the lab's chalkos image to the lab's registry.
          chalkos.packages.${system}.chalkbuild
        ];
      };
    };
}
